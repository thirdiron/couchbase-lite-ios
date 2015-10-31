//
//  CBLTimeSeries.h
//  CouchbaseLite
//
//  Created by Jens Alfke on 10/26/15.
//  Copyright © 2015 Couchbase, Inc. All rights reserved.
//

#import "CBLBase.h"
@class CBLDatabase, CBLReplication;


NS_ASSUME_NONNULL_BEGIN

/** Efficiently stores small time-stamped JSON values into a database,
    and can replicate them to a server (purging them as soon as they're pushed.) */
@interface CBLTimeSeries : NSObject

- (nullable instancetype) initWithDatabase: (CBLDatabase*)db
                                   docType: (NSString*)docType
                                     error: (NSError**)outError;

/** Adds an event, timestamped with the current time. */
- (void) addEvent: (NSDictionary*)event;

/** Adds an event with a custom timestamp (which should be greater than the last timestamp.) */
- (void) addEvent: (NSDictionary*)event atTime: (CFAbsoluteTime)time;

/** Writes all pending events to documents, then calls the onFlushed block. */
- (void) flush: (void(^)())onFlushed;


//// REPLICATION:

/** Creates, but does not start, a new CBLReplication to push the events to a remote database.
    You can customize the replication's properties before starting it, but don't alter the
    filter or remove the existing customProperties. */
- (CBLReplication*) createPushReplication: (NSURL*)remoteURL
                          purgeWhenPushed: (BOOL)purgeWhenPushed;


//// QUERYING:

/** Enumerates the events from time t0 to t1, inclusive. Each event is an NSDictionary,
    as provided to -addEvent, with a key "t" whose value is the absolute time as an
    NSDate.
    @param t0  The starting time (or 0 to start from the beginning.)
    @param t1  The ending time (or 0 to continue till the end.)
    @return  An enumerator of NSDictionaries, one per event. */
- (nullable NSEnumerator<NSDictionary*>*) eventsFromTime: (CFAbsoluteTime)t0
                                                  toTime: (CFAbsoluteTime)t1
                                                   error: (NSError**)outError;

@end

NS_ASSUME_NONNULL_END
