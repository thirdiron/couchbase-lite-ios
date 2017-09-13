//
//  CBLDatabase.m
//  CouchbaseLite
//
//  Created by Jens Alfke on 6/17/12.
//  Copyright (c) 2012-2013 Couchbase, Inc. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

#import "CouchbaseLitePrivate.h"
#import "CBLDatabase.h"
#import "CBLDatabase+Internal.h"
#import "CBLDatabase+Insertion.h"
#import "CBLDatabaseChange.h"
#import "CBL_Shared.h"
#import "CBLInternal.h"
#import "CBLModel_Internal.h"
#import "CBLModelFactory.h"
#import "CBLCache.h"
#import "CBLManager+Internal.h"
#import "CBLMisc.h"
#import "CBLSymmetricKey.h"
#import "MYAction.h"
#import "MYBlockUtils.h"
#import "ExceptionUtils.h"

#if TARGET_OS_IPHONE
#import <UIKit/UIApplication.h>
#endif


// NOTE: This file contains mostly just public-API method implementations.
// The lower-level stuff is in CBLDatabase+Internal.m, etc.


// Size of document cache: max # of otherwise-unreferenced docs that will be kept in memory.
#define kDocRetainLimit 50

NSString* const kCBLDatabaseChangeNotification = @"CBLDatabaseChange";


static id<CBLFilterCompiler> sFilterCompiler;


@implementation CBLDatabase
{
    CBLCache* _docCache;
    NSMutableSet* _allReplications;
}


@synthesize manager=_manager, unsavedModelsMutable=_unsavedModelsMutable;
@synthesize dir=_dir, name=_name, isOpen=_isOpen;


- (instancetype) initWithDir: (NSString*)dir
                        name: (NSString*)name
                     manager: (CBLManager*)manager
                    readOnly: (BOOL)readOnly
{
    self = [self _initWithDir: dir name: name manager: manager readOnly: readOnly];
    if (self) {
        _unsavedModelsMutable = [NSMutableSet set];
        _allReplications = [[NSMutableSet alloc] init];
#if TARGET_OS_IPHONE
        [[NSNotificationCenter defaultCenter] addObserver: self
                                                 selector: @selector(appBackgrounding:)
                                                     name: UIApplicationWillTerminateNotification
                                                   object: nil];
        // Also clean up when app is backgrounded, on iOS:
        [[NSNotificationCenter defaultCenter] addObserver: self
                                                 selector: @selector(appBackgrounding:)
                                                     name: UIApplicationDidEnterBackgroundNotification
                                                   object: nil];
#endif
    }
    return self;
}


- (void)dealloc {
    if (_isOpen) {
        Assert(!_manager);
        [self _close];
    }
    [[NSNotificationCenter defaultCenter] removeObserver: self];
}


- (NSUInteger) documentCount {
    return _storage.documentCount;
}


- (SequenceNumber) lastSequenceNumber {
    return _storage.lastSequence;
}


- (void) postPublicChangeNotification: (NSArray*)changes {
    BOOL external = NO;
    for (CBLDatabaseChange* change in changes) {
        // Notify the corresponding instantiated CBLDocument object (if any):
        [[self _cachedDocumentWithID: change.documentID] revisionAdded: change notify: YES];
        if (change.source != nil)
            external = YES;
    }

    // Post the public kCBLDatabaseChangeNotification:
    NSDictionary* userInfo = @{@"changes": changes,
                               @"external": @(external)};
    NSNotification* n = [NSNotification notificationWithName: kCBLDatabaseChangeNotification
                                                      object: self
                                                    userInfo: userInfo];
    [self postNotification:n];
}


#if TARGET_OS_IPHONE
- (void) appBackgrounding: (NSNotification*)n {
    [self doAsync: ^{
        [self autosaveAllModels: nil];
    }];
}
#endif


- (NSURL*) internalURL {
    return [_manager.internalURL URLByAppendingPathComponent: self.name isDirectory: YES];
}


static void catchInBlock(void (^block)()) {
    @try {
        block();
    }catchAndReport(@"-[CBLDatabase doAsync:]");
}


- (void) doAsync: (void (^)(void))block {
    block = ^{
        if (_isOpen)
            catchInBlock(block);
    };
    if (_dispatchQueue)
        dispatch_async(_dispatchQueue, block);
    else
        MYOnThreadInModes(_thread, CBL_RunloopModes, NO, block);
}


- (void) doSync: (void (^)(void))block {
    if (_dispatchQueue)
        dispatch_sync(_dispatchQueue, ^{catchInBlock(block);});
    else
        MYOnThreadInModes(_thread, CBL_RunloopModes, YES, ^{catchInBlock(block);});
}


- (void) doAsyncAfterDelay: (NSTimeInterval)delay block: (void (^)())block {
    block = ^{
        if (_isOpen)
            catchInBlock(block);
    };
    if (_dispatchQueue) {
        dispatch_time_t popTime = dispatch_time(DISPATCH_TIME_NOW, (int64_t)(delay * NSEC_PER_SEC));
        dispatch_after(popTime, _dispatchQueue, block);
    } else {
        //FIX: This schedules on the _current_ thread, not _thread!
        MYAfterDelay(delay, block);
    }
}


- (BOOL) waitFor: (BOOL (^)())block {
    if (_dispatchQueue) {
        Warn(@"-[CBLDatabase waitFor:] cannot be used with dispatch queues, only runloops");
        return NO;
    }
    return MYWaitFor(CBL_PrivateRunloopMode, block);
}


- (BOOL) inTransaction: (BOOL(^)(void))block {
    return 200 == [_storage inTransaction: ^CBLStatus {
        return block() ? 200 : 999;
    }];
}


- (BOOL) close: (NSError**)outError {
    if (![self saveAllModels: outError])
        return NO;
    for (CBLReplication* repl in self.allReplications)
        [repl stop];
    [self _close];
    return YES;
}


- (BOOL) deleteDatabase: (NSError**)outError {
    if (_readOnly)
        return CBLStatusToOutNSError(kCBLStatusForbidden, outError);
    LogTo(Database, @"Deleting %@", _dir);
    [[NSNotificationCenter defaultCenter] postNotificationName: CBL_DatabaseWillBeDeletedNotification
                                                        object: self];
    [self _close];

    // Wait for all threads to close this database file:
    [_manager.shared forgetDatabaseNamed: _name];

    if (!self.exists) {
        return YES;
    }
    return [[self class] deleteDatabaseFilesAtPath: _dir error: outError];
}


- (BOOL) compact: (NSError**)outError {
    if (_readOnly)
        return CBLStatusToOutNSError(kCBLStatusForbidden, outError);
    //FIX:
//    CBLStatus status = [_storage inTransaction: ^CBLStatus {
        // Do this in a transaction because garbageCollectAttachments expects the database to be
        // freshly compacted (i.e. only current revisions have bodies), and it could delete new
        // attachments added while it's working. So lock out other writers for the duration.
    return [_storage compact: outError] && [self garbageCollectAttachments: outError];
//    }];
//    return !CBLStatusIsError(status);
}

- (NSUInteger) maxRevTreeDepth {
    return _storage.maxRevTreeDepth;
}

- (void) setMaxRevTreeDepth: (NSUInteger)maxRevs {
    if (maxRevs == 0)
        maxRevs = _manager.defaultMaxRevTreeDepth;
    if (maxRevs != _storage.maxRevTreeDepth) {
        _storage.maxRevTreeDepth = (unsigned)maxRevs;
        [_storage setInfo: $sprintf(@"%lu", (unsigned long)maxRevs) forKey: @"max_revs"];
    }
}


- (BOOL) replaceUUIDs: (NSError**)outError {
    CBLStatus status = [_storage setInfo: CBLCreateUUID() forKey: @"publicUUID"];
    if (status == kCBLStatusOK)
        status = [_storage setInfo: CBLCreateUUID() forKey: @"privateUUID"];
    if (status == kCBLStatusOK)
        return YES;

    CBLStatusToOutNSError(status, outError);
    return NO;
}


- (BOOL) changeEncryptionKey: (id)newKeyOrPassword error: (NSError**)outError {
    if (_readOnly)
        return CBLStatusToOutNSError(kCBLStatusForbidden, outError);
    if (![_storage respondsToSelector: @selector(actionToChangeEncryptionKey:)])
        return CBLStatusToOutNSError(kCBLStatusNotImplemented, outError);

    CBLSymmetricKey* newKey = nil;
    if (newKeyOrPassword) {
        newKey = [[CBLSymmetricKey alloc] initWithKeyOrPassword: newKeyOrPassword];
        if (!newKey)
            return CBLStatusToOutNSError(kCBLStatusBadRequest, outError);
    }
    
    // Backup the old encryption key:
    id oldKey = [_manager.shared valueForType: @"encryptionKey" name: @"" inDatabaseNamed: _name];
    
    // Notify that the database will be rekeyed:
    [[NSNotificationCenter defaultCenter] postNotificationName: CBL_DatabaseWillBeRekeyedNotification
                                                        object: self];
    
    // Close the database:
    [self _close];
    
    // Wait for all threads to close this database file:
    [_manager.shared forgetDatabaseNamed: _name];
    
    // Restore the old encryption key
    [_manager registerEncryptionKey: oldKey forDatabaseNamed: _name];
    
    // Reopen the database:
    NSError* error;
    if (![self open: &error]) {
        NSString *mesg = [NSString stringWithFormat: @"Cannot reopen the database to change the "
                            "encryption key: %@", error];
        if (outError)
            *outError = CBLStatusToNSErrorWithInfo(kCBLStatusServiceUnavailable, mesg, nil, nil);
        return NO;
    }

    MYAction* action = [_storage actionToChangeEncryptionKey: newKey];
    if (!action)
        return CBLStatusToOutNSError(kCBLStatusNotImplemented, outError);
    [action addAction: [_attachments actionToChangeEncryptionKey: newKey]];
    [action addPerform:^BOOL(NSError** error) {
        [_manager registerEncryptionKey: newKeyOrPassword forDatabaseNamed: _name];
        return YES;
    } backOut: nil cleanUp: nil];
    return [action run: outError];
}


#pragma mark - DOCUMENTS:


- (CBLDocument*) documentWithID: (NSString*)docID mustExist: (BOOL)mustExist isNew: (BOOL)isNew {
    CBLDocument* doc = (CBLDocument*) [_docCache resourceWithCacheKey: docID];
    if (doc) {
        if (mustExist && doc.currentRevision == nil)  // loads current revision from db
            return nil;
        return doc;
    }
    if (docID.length == 0)
        return nil;
    doc = [[CBLDocument alloc] initWithDatabase: self
                                     documentID: docID
                                         exists: !isNew];
    if (!doc)
        return nil;
    if (mustExist && doc.currentRevision == nil)  // loads current revision from db
        return nil;
    if (!_docCache)
        _docCache = [[CBLCache alloc] initWithRetainLimit: kDocRetainLimit];
    [_docCache addResource: doc];
    return doc;
}


- (CBLDocument*) documentWithID: (NSString*)docID {
    return [self documentWithID: docID mustExist: NO isNew: NO];
}

- (CBLDocument*) existingDocumentWithID: (NSString*)docID {
    return [self documentWithID: docID mustExist: YES isNew: NO];
}

- (CBLDocument*) objectForKeyedSubscript: (NSString*)key {
    return [self documentWithID: key mustExist: NO isNew: NO];
}

- (CBLDocument*) createDocument {
    return [self documentWithID: [[self class] generateDocumentID] mustExist: NO isNew: YES];
}


- (CBLDocument*) _cachedDocumentWithID: (NSString*)docID {
    return (CBLDocument*) [_docCache resourceWithCacheKeyDontRecache: docID];
}

- (void) _clearDocumentCache {
    [_docCache forgetAllResources];
}

- (void) _pruneDocumentCache {
    [_docCache unretainResources];
}

- (void) removeDocumentFromCache: (CBLDocument*)document {
    [_docCache forgetResource: document];
}


- (CBLQuery*) createAllDocumentsQuery {
    return [[CBLQuery alloc] initWithDatabase: self view: nil];
}


static NSString* makeLocalDocID(NSString* docID) {
    return [@"_local/" stringByAppendingString: docID];
}


- (NSDictionary*) existingLocalDocumentWithID: (NSString*)localDocID {
    return [_storage getLocalDocumentWithID: makeLocalDocID(localDocID) revisionID: nil].properties;
}

- (BOOL) putLocalDocument: (NSDictionary*)properties
                   withID: (NSString*)localDocID
                    error: (NSError**)outError
{
    localDocID = makeLocalDocID(localDocID);
    CBL_MutableRevision* rev = [[CBL_MutableRevision alloc] initWithDocID: localDocID
                                                                    revID: nil
                                                                  deleted: (properties == nil)];
    if (properties)
        rev.properties = properties;
    // Now update the doc (or delete it, if properties is nil):
    CBLStatus status;
    BOOL ok = [_storage putLocalRevision: rev
                          prevRevisionID: nil
                                obeyMVCC: NO
                                  status: &status] != nil;
    if (!ok)
        CBLStatusToOutNSError(status, outError);
    return ok;
}

- (BOOL) deleteLocalDocumentWithID: (NSString*)localDocID error: (NSError**)outError {
    return [self putLocalDocument: nil withID: localDocID error: outError];
}


#pragma mark - VIEWS:


- (CBLView*) registerView: (CBLView*)view {
    if (!view)
        return nil;
    if (!_views)
        _views = [[NSMutableDictionary alloc] init];
    _views[view.name] = view;
    return view;
}


- (CBLView*) existingViewNamed: (NSString*)name {
    CBLView* view = _views[name];
    if (view)
        return view;
    return [self registerView: [[CBLView alloc] initWithDatabase: self name: name create: NO]];
}


- (CBLView*) viewNamed: (NSString*)name {
    CBLView* view = _views[name];
    if (view)
        return view;
    return [self registerView: [[CBLView alloc] initWithDatabase: self name: name create: YES]];
}


- (CBLQuery*) slowQueryWithMap: (CBLMapBlock)mapBlock {
    return [[CBLQuery alloc] initWithDatabase: self mapBlock: mapBlock];
}


#pragma mark - VALIDATION & FILTERS:


- (void) setValidationNamed: (NSString*)validationName asBlock: (CBLValidationBlock)validationBlock {
    [self.shared setValue: [validationBlock copy]
                  forType: @"validation" name: validationName inDatabaseNamed: _name];
}

- (CBLValidationBlock) validationNamed: (NSString*)validationName {
    return [self.shared valueForType: @"validation" name: validationName inDatabaseNamed: _name];
}


- (void) setFilterNamed: (NSString*)filterName asBlock: (CBLFilterBlock)filterBlock {
    [self.shared setValue: [filterBlock copy]
                  forType: @"filter" name: filterName inDatabaseNamed: _name];
}

- (CBLFilterBlock) filterNamed: (NSString*)filterName {
    return [self.shared valueForType: @"filter" name: filterName inDatabaseNamed: _name];
}


+ (void) setFilterCompiler: (id<CBLFilterCompiler>)compiler {
    sFilterCompiler = compiler;
}

+ (id<CBLFilterCompiler>) filterCompiler {
    return sFilterCompiler;
}


#pragma mark - REPLICATION:


- (NSArray*) allReplications {
    return [_allReplications allObjects];
}

- (void) addReplication: (CBLReplication*)repl {
    [_allReplications addObject: repl];
}

- (void) forgetReplication: (CBLReplication*)repl {
    [_allReplications removeObject: repl];
}


- (CBLReplication*) createPushReplication: (NSURL*)url {
    return [[CBLReplication alloc] initWithDatabase: self remote: url pull: NO];
}

- (CBLReplication*) createPullReplication: (NSURL*)url {
    return [[CBLReplication alloc] initWithDatabase: self remote: url pull: YES];
}

- (CBLReplication*) existingReplicationWithURL: (NSURL*)url pull: (BOOL)pull {
    for (CBLReplication* repl in _allReplications)
        if (repl.pull == pull && $equal(repl.remoteURL, url))
            return repl;
    return nil;
}


@end




@implementation CBLDatabase (CBLModel)

- (NSArray*) unsavedModels {
    if (_unsavedModelsMutable.count == 0)
        return nil;
    return [_unsavedModelsMutable allObjects];
}

- (BOOL) saveAllModels: (NSError**)outError {
    NSArray* unsaved = self.unsavedModels;
    if (unsaved.count == 0)
        return YES;
    return [CBLModel saveModels: unsaved error: outError];
}


- (BOOL) autosaveAllModels: (NSError**)outError {
    NSArray* unsaved = [self.unsavedModels my_filter: ^int(CBLModel* model) {
        return model.autosaves;
    }];
    if (unsaved.count == 0)
        return YES;
    return [CBLModel saveModels: unsaved error: outError];
}


@end



@implementation CBLDatabase (CBLModelFactory)

- (CBLModelFactory*) modelFactory {
    if (!_modelFactory)
        _modelFactory = [[CBLModelFactory alloc] init];
    return _modelFactory;
}

- (void) setModelFactory:(CBLModelFactory *)modelFactory {
    _modelFactory = modelFactory;
}

@end
