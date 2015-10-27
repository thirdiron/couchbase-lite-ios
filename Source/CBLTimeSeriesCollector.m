//
//  CBLTimeSeriesCollector.m
//  CouchbaseLite
//
//  Created by Jens Alfke on 10/26/15.
//  Copyright © 2015 Couchbase, Inc. All rights reserved.
//

#import "CBLTimeSeriesCollector.h"
#import <CouchbaseLite/CouchbaseLite.h>
#import <stdio.h>


#define kMaxDocSize         (100*1024)  // Max length in bytes of a document
#define kMaxDocEventCount   1000        // Max number of events to pack into a document


@implementation CBLTimeSeriesCollector
{
    dispatch_queue_t _queue;
    FILE *_out;
    NSUInteger _eventsInFile;
    CBLDatabase* _db;
    NSString* _docIDPrefix;
}


static NSTimeInterval kToUnixTime;


+ (void) initialize {
    if (self == [CBLJSON class]) {
        kToUnixTime = [[NSDate dateWithTimeIntervalSinceReferenceDate: 0.0]
                                                                    timeIntervalSince1970];
    }
}


- (instancetype) initWithPath: (NSString*)path
                     database: (CBLDatabase*)db
                  docIDPrefix: (NSString*)docIDPrefix
                        error: (NSError**)outError
{
    self = [super init];
    if (self) {
        _out = fopen(path.fileSystemRepresentation, "a+"); // append-only, and read
        if (!_out) {
            if (outError)
                *outError = [NSError errorWithDomain: NSPOSIXErrorDomain code: errno userInfo:nil];
            return nil;
        }
        _queue = dispatch_queue_create("CBLTimeSeriesCollector", DISPATCH_QUEUE_SERIAL);
        _db = db;
        _docIDPrefix = [docIDPrefix copy];
    }
    return self;
}


- (void) stop {
    if (_queue) {
        dispatch_sync(_queue, ^{
            if (_out)
                fclose(_out);
            _queue = nil;
        });
    }
}


- (void) dealloc {
    [self stop];
}


- (void) addEvent: (NSDictionary*)event {
    [self addEvent: event atTime: CFAbsoluteTimeGetCurrent()];
}


- (void) addEvent: (NSDictionary*)event atTime: (CFAbsoluteTime)time {
    Assert(event);
    dispatch_async(_queue, ^{
        uint64_t timestamp = (uint64_t)((time + kToUnixTime)*1000.0);
        NSMutableDictionary* props = [event mutableCopy];
        props[@"t"] = @(timestamp);
        NSData* json = [CBLJSON dataWithJSONObject: props options: 0 error: NULL];
        Assert(json);

        off_t pos = ftell(_out);
        if (pos + json.length + 20 > kMaxDocSize || _eventsInFile >= kMaxDocEventCount) {
            // Write events to a CBL document:
            [self transferToDB];
            // Now erase the file for subsequent events:
            fseek(_out, 0, SEEK_SET);
            ftruncate(fileno(_out), 0);
            pos = 0;
            _eventsInFile = 0;
        }

        if (fputs(pos==0 ? "[" : ",\n", _out) < 0
                || fwrite(json.bytes, json.length, 1, _out) < 1
                || fflush(_out) < 0)
        {
            //TODO: error
        }
    });
}

- (void) transferToDB {
    if (fputs("]", _out) < 0 || fflush(_out) < 0) {
        //TODO: ERROR
    }

    // Parse a JSON array from the (memory-mapped) file:
    off_t length = ftell(_out);
    void* mapped = mmap(NULL, length, PROT_READ, MAP_PRIVATE, fileno(_out), 0);
    NSData* json = [[NSData alloc] initWithBytesNoCopy: mapped length: length freeWhenDone: NO];
    NSArray* events = [CBLJSON JSONObjectWithData: json options: 0 error: NULL];
    munmap(mapped, length);

    NSUInteger count = events.count;
    for (NSUInteger pos = 0; pos < count; pos += kMaxDocEventCount) {
        NSRange range = {pos, MIN(kMaxDocEventCount, count-pos)};
        NSArray* group = [events subarrayWithRange: range];
        [self addEventsToDB: group];
    }
}


#if 0 // this old version reads/parses a line at a time
- (void) transferToDB {
        fseek(_out, 0, SEEK_SET);
        BOOL atEnd = NO;
        while(!atEnd) {
            NSMutableArray* events = [[NSMutableArray alloc] initWithCapacity: kMaxDocEventCount];
            @autoreleasepool {
                while (events.count < kMaxDocEventCount) {
                    // Read the next line:
                    size_t len;
                    char* line = fgetln(_out, &len);
                    if (!line) {
                        //TODO: Error
                        return;
                    }
                    if (len == 0 || line[len-1] != '\n') {
                        atEnd = YES;
                        break;
                    }

                    NSData* json = [[NSData alloc] initWithBytesNoCopy: line length: len
                                                          freeWhenDone: NO];
                    NSDictionary* props = [CBLJSON JSONObjectWithData: json options: 0 error: NULL];
                    [events addObject: props];
                }
            }
            [self addEventsToDB: events];
        }
}
#endif


- (void) addEventsToDB: (NSArray*)events {
    if (events.count == 0)
        return;
    uint64_t timestamp = [[events[0] objectForKey: @"t"] unsignedLongLongValue];
    NSString* docID = [_docIDPrefix stringByAppendingFormat: @"-%08llx", timestamp];
    [_db doAsync:^{
        // On database thread/queue:
        NSError* error;
        if (![_db[docID] putProperties: @{@"events": events} error: &error])
            Warn(@"CBLTimeSeriesCollector: Couldn't save events to '%@': %@", docID, error);
    }];
}


- (CBLReplication*) createPushReplication: (NSURL*)remoteURL {
    CBLReplication* push = [_db createPushReplication: remoteURL];
    [_db setFilterNamed: @"com.couchbase.DocIDPrefix"
                asBlock: ^BOOL(CBLSavedRevision *revision, NSDictionary *params) {
        return [revision.document.documentID hasPrefix: params[@"prefix"]];
    }];
    push.filter = @"com.couchbase.DocIDPrefix";
    push.filterParams = @{@"prefix": [_docIDPrefix stringByAppendingString: @"-"]};
    push.customProperties = @{@"allNew": @YES, @"purgePushed": @YES};
    return push;
}


@end
