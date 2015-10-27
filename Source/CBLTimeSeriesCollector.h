//
//  CBLTimeSeriesCollector.h
//  CouchbaseLite
//
//  Created by Jens Alfke on 10/26/15.
//  Copyright © 2015 Couchbase, Inc. All rights reserved.
//

#import <Foundation/Foundation.h>
@class CBLDatabase, CBLReplication;


/** Efficiently stores small time-stamped JSON values into a database,
    and can replicate them to a server (purging them as soon as they're pushed.) */
@interface CBLTimeSeriesCollector : NSObject

- (instancetype) initWithPath: (NSString*)path
                     database: (CBLDatabase*)db
                  docIDPrefix: (NSString*)docIDPrefix
                        error: (NSError**)outError;

/** Adds an event, timestamped with the current time. */
- (void) addEvent: (NSDictionary*)event;

/** Adds an event with a custom timestamp (which should be greater than the last timestamp.) */
- (void) addEvent: (NSDictionary*)event atTime: (CFAbsoluteTime)time;

/** Creates, but does not start, a new CBLReplication to push the events to a remote database.
    You can customize the replication's properties before starting it, but don't alter the
    filter or remove the existing customProperties. */
- (CBLReplication*) createPushReplication: (NSURL*)remoteURL;

@end
