//
//  EntityManager.h
//
//  Created by Ufuk Kayserilioglu on 11/09/2011.
//  Copyright 2011 Ufuk Kayserilioglu. All rights reserved.
//

#include "objc/runtime.h"
#include "FMDatabase.h"
#import <Foundation/Foundation.h>

@protocol EntityDescriptor
+ (int) version;
+ (SEL) primaryKeyColumn;
@optional
+ (NSArray *) transientProperties;
+ (NSArray *) compositeKeyColumns;
@end

@protocol EntityManagerDelegate
- (NSString*)foundVersionMismatchOnEntity:(Class)_entityClass dbVersion:(NSNumber *)_dbVersion entityVersion:(NSNumber *)_entityVersion;
@end

@interface EntityManager : NSObject {
	FMDatabase * db;
}

@property (readonly, retain) FMDatabase * db;

+ (void) registerClass:(Class)_class;
+ (EntityManager *) openManagerWithDatabase:(NSString *)databasePath delegate:(id <EntityManagerDelegate>)_delegate;
- (void) closeManager;

- (void) replace:(id)instance;
- (void) create:(id)instance;
- (void) update:(id)instance;
- (void) remove:(id)instance;

- (void) removeWithClass:(Class)_class andPrimaryKey:(id)key;
- (void) removeWithClass:(Class)_class andPrimaryKeyList:(NSArray *)keys;
- (void) removeWithClass:(Class)_class andQuery:(id)query, ...;

- (id) loadWithClass:(Class)_class andPrimaryKey:(id)key;
- (id) loadSingleWithClass:(Class)_class andQuery:(id)query, ...;
- (NSArray *) loadArrayWithClass:(Class)_class andQuery:(id)query, ...;

- (BOOL)hasEntityForClass:(Class)_class andQuery:(NSString *)query, ...;

@end
