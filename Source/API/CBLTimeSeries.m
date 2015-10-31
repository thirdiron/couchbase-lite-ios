//
//  CBLTimeSeries.m
//  CouchbaseLite
//
//  Created by Jens Alfke on 10/26/15.
//  Copyright © 2015 Couchbase, Inc. All rights reserved.
//

#import "CBLTimeSeries.h"
#import "CBLDatabase+Internal.h"
#import <CouchbaseLite/CouchbaseLite.h>
#import <stdio.h>
#import <sys/mman.h>


#define kMaxDocSize         (100*1024)  // Max length in bytes of a document
#define kMaxDocEventCount   1000u       // Max number of events to pack into a document


// Internal enumerator implementation whose -nextObject method just calls a block
@interface CBLTimeSeriesEnumerator : NSEnumerator<NSDictionary*>
- (id) initWithBlock: (NSDictionary*(^)())block;
@end




@implementation CBLTimeSeries
{
    dispatch_queue_t _queue;
    FILE *_out;
    NSError* _error;
    NSUInteger _eventsInFile;
    CBLDatabase* _db;
    NSString* _docType;
}


static NSTimeInterval kToUnixTime;


static uint64_t encodeTime(CFAbsoluteTime time) {
    return (uint64_t)((time + kToUnixTime)*1000.0);
}

static CFAbsoluteTime decodeTime(uint64_t time) {
    return time/1000.0 - kToUnixTime;
}

+ (void) initialize {
    if (self == [CBLTimeSeries class]) {
        kToUnixTime = [[NSDate dateWithTimeIntervalSinceReferenceDate: 0.0] timeIntervalSince1970];
    }
}


- (instancetype) initWithDatabase: (CBLDatabase*)db
                          docType: (NSString*)docType
                            error: (NSError**)outError
{
    NSParameterAssert(db);
    NSParameterAssert(docType);
    self = [super init];
    if (self) {
        NSString* filename = [NSString stringWithFormat: @"TS-%@.tslog", docType];
        NSString* path = [db.dir stringByAppendingPathComponent: filename];
        _out = fopen(path.fileSystemRepresentation, "a+"); // append-only, and read
        if (!_out) {
            if (outError)
                *outError = [NSError errorWithDomain: NSPOSIXErrorDomain code: errno userInfo:nil];
            return nil;
        }
        _queue = dispatch_queue_create("CBLTimeSeries", DISPATCH_QUEUE_SERIAL);
        _db = db;
        _docType = [docType copy];
    }
    return self;
}


- (void) dealloc {
    [self stop];
}


- (void) stop {
    if (_queue) {
        dispatch_sync(_queue, ^{
            if (_out) {
                fclose(_out);
                _out = NULL;
            }
        });
        _queue = nil;
    }
    _error = nil;
}


- (BOOL) checkError: (int)err {
    if (err == 0)
        return NO;
    Warn(@"CBLTimeSeries: POSIX error %d", err);
    if (!_error) {
        _error = [NSError errorWithDomain: NSPOSIXErrorDomain code: err
                                 userInfo: @{NSLocalizedDescriptionKey: @(strerror(err))}];
    }
    return YES;
}


- (BOOL) checkWriteError {
    return [self checkError: ferror(_out)];
}


- (void) addEvent: (NSDictionary*)event {
    [self addEvent: event atTime: CFAbsoluteTimeGetCurrent()];
}


- (void) addEvent: (NSDictionary*)event atTime: (CFAbsoluteTime)time {
    Assert(event);
    dispatch_async(_queue, ^{
        NSMutableDictionary* props = [event mutableCopy];
        props[@"t"] = @(encodeTime(time));
        NSData* json = [CBLJSON dataWithJSONObject: props options: 0 error: NULL];
        Assert(json);

        off_t pos = ftell(_out);
        if (pos + json.length + 20 > kMaxDocSize || _eventsInFile >= kMaxDocEventCount) {
            [self transferToDB];
            pos = 0;
        }

        if (fputs(pos==0 ? "[" : ",\n", _out) < 0
                || fwrite(json.bytes, json.length, 1, _out) < 1
                || fflush(_out) < 0)
        {
            [self checkWriteError];
        }
        ++_eventsInFile;
    });
}


- (void) flush: (void(^)())onFlushed {
    dispatch_async(_queue, ^{
        if (_eventsInFile > 0 || ftell(_out) > 0) {
            [self transferToDB];
        }
        [_db doAsync: onFlushed];
    });
}


- (BOOL) transferToDB {
    if (fputs("]", _out) < 0 || fflush(_out) < 0) {
        [self checkWriteError];
        return NO;
    }

    // Parse a JSON array from the (memory-mapped) file:
    size_t length = ftell(_out);
    void* mapped = mmap(NULL, length, PROT_READ, MAP_PRIVATE, fileno(_out), 0);
    if (!mapped)
        return [self checkError: errno];
    NSData* json = [[NSData alloc] initWithBytesNoCopy: mapped length: length freeWhenDone: NO];
    NSError* jsonError;
    NSArray* events = [CBLJSON JSONObjectWithData: json options: 0 error: &jsonError];
    munmap(mapped, length);
    if (jsonError) {
        if (!_error)
            _error = jsonError;
        return NO;
    }

    // Add the events to documents in batches:
    NSUInteger count = events.count;
    for (NSUInteger pos = 0; pos < count; pos += kMaxDocEventCount) {
        NSRange range = {pos, MIN(kMaxDocEventCount, count-pos)};
        NSArray* group = [events subarrayWithRange: range];
        [self addEventsToDB: group];
    }

    // Now erase the file for subsequent events:
    fseek(_out, 0, SEEK_SET);
    ftruncate(fileno(_out), 0);
    _eventsInFile = 0;
    return YES;
}


- (NSString*) docIDForTimestamp: (uint64_t)timestamp {
    return [NSString stringWithFormat: @"TS-%@-%08llx", _docType, timestamp];
}


- (void) addEventsToDB: (NSArray*)events {
    if (events.count == 0)
        return;
    uint64_t timestamp = [[events[0] objectForKey: @"t"] unsignedLongLongValue];
    NSString* docID = [self docIDForTimestamp: timestamp];
    [_db doAsync:^{
        // On database thread/queue:
        NSError* error;
        if (![_db[docID] putProperties: @{@"type": _docType, @"events": events} error: &error])
            Warn(@"CBLTimeSeries: Couldn't save events to '%@': %@", docID, error);
    }];
}


#pragma mark - REPLICATION:


- (CBLReplication*) createPushReplication: (NSURL*)remoteURL
                          purgeWhenPushed: (BOOL)purgeWhenPushed
{
    CBLReplication* push = [_db createPushReplication: remoteURL];
    [_db setFilterNamed: @"com.couchbase.DocIDPrefix"
                asBlock: ^BOOL(CBLSavedRevision *revision, NSDictionary *params) {
        return [revision.document.documentID hasPrefix: params[@"prefix"]];
    }];
    push.filter = @"com.couchbase.DocIDPrefix";
    push.filterParams = @{@"prefix": [NSString stringWithFormat: @"TS-%@-", _docType]};
    push.customProperties = @{@"allNew": @YES, @"purgePushed": @(purgeWhenPushed)};
    return push;
}


#pragma mark - QUERYING:


// Returns an array of events starting at time t0 and continuing to the end of the document.
// If t0 is before the earliest recorded timestamp, or falls between two documents, returns @[].
- (NSArray*) eventsFromDocForTime: (CFAbsoluteTime)t0 error: (NSError**)outError {
    // Get the doc containing time t0. To do this we have to search _backwards_ since the doc ID
    // probably has a timestamp before t0:
    CBLQuery* q = [_db createAllDocumentsQuery];
    uint64_t timestamp0 = t0 ? encodeTime(t0) : 0;
    q.startKey = [self docIDForTimestamp: timestamp0];
    q.descending = YES;
    q.limit = 1;
    q.prefetch = YES;
    __block CBLQueryEnumerator* e = [q run: outError];
    if (!e)
        return nil;
    CBLQueryRow* row = e.nextRow;
    if (!row)
        return @[];
    NSArray* events = row.document[@"events"];

    // Now find the first event with t ≥ t0:
    NSUInteger i0;
    i0 = [events indexOfObject: @{@"t": @(timestamp0)}
                 inSortedRange: NSMakeRange(0, events.count)
                       options: NSBinarySearchingFirstEqual | NSBinarySearchingInsertionIndex
               usingComparator: ^NSComparisonResult(id obj1, id obj2) {
                   return [[obj1 objectForKey: @"t"]  compare: [obj2 objectForKey: @"t"]];
               }];
    if (i0 == NSNotFound)
        return @[];
    return [events subarrayWithRange: NSMakeRange(i0, events.count-i0)];
}


- (NSEnumerator<NSDictionary*>*) eventsFromTime: (CFAbsoluteTime)t0
                                         toTime: (CFAbsoluteTime)t1
                                          error: (NSError**)outError
{
    // Get the first series from the doc containing t0 (if any):
    __block NSArray* curSeries = nil;
    if (t0 > 0.0) {
        curSeries = [self eventsFromDocForTime: t0 error: outError];
        if (!curSeries)
            return nil;
    }

    // Start forwards query if I haven't already:
    CBLQuery* q = [_db createAllDocumentsQuery];
    uint64_t startStamp;
    if (curSeries.count > 0) {
        startStamp = [[curSeries.lastObject objectForKey: @"t"] unsignedLongLongValue];
        q.inclusiveStart = NO;
    } else {
        startStamp = t0 > 0 ? encodeTime(t0) : 0;
    }
    uint64_t endStamp = t1 ? encodeTime(t1) : UINT64_MAX;

    CBLQueryEnumerator* e;
    if (startStamp < endStamp) {
        q.startKey = [self docIDForTimestamp: startStamp];
        q.endKey   = [self docIDForTimestamp: endStamp];
        e = [q run: outError];
        if (!e)
            return nil;
    }

    // OK, here is the block for the enumerator:
    __block NSUInteger curIndex = 0;
    return [[CBLTimeSeriesEnumerator alloc] initWithBlock: ^NSDictionary*{
        while (curIndex >= curSeries.count) {
            // Go to the next document:
            CBLQueryRow* row = e.nextRow;
            if (!row)
                return nil;
            curSeries = row.document[@"events"];
            curIndex = 0;
        }
        // Return the next event from curSeries:
        NSDictionary* event = curSeries[curIndex++];
        uint64_t timestamp = [event[@"t"] unsignedLongLongValue];
        if (timestamp > endStamp) {
            return nil;
        }
        NSMutableDictionary* result = [event mutableCopy];
        result[@"t"] = [NSDate dateWithTimeIntervalSinceReferenceDate: decodeTime(timestamp)];
        return result;
    }];
}


@end



// Internal enumerator implementation whose -nextObject method just calls a block
@implementation CBLTimeSeriesEnumerator
{
    NSDictionary* (^_block)();
}

- (id) initWithBlock: (NSDictionary*(^)())block
{
    self = [super init];
    if (self) {
        _block = block;
    }
    return self;
}

- (NSDictionary*) nextObject {
    if (!_block)
        return nil;
    id result = _block();
    if (!result)
        _block = nil;
    return result;
}

@end
