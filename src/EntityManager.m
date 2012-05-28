//
//  EntityManager.m
//
//  Created by Ufuk Kayserilioglu on 11/09/2011.
//  Copyright 2011 Ufuk Kayserilioglu. All rights reserved.
//

#import "EntityManager.h"
#import "objc/runtime.h"
#import "FMDatabase.h"
#import "FMDatabaseAdditions.h"

static NSMutableDictionary * structureMap = nil;
static NSMutableDictionary * databaseTypeMap = nil;

static const char *getPropertyType(objc_property_t property) {
    const char *attributes = property_getAttributes(property);
    char buffer[1 + strlen(attributes)];
    strcpy(buffer, attributes);
    char *state = buffer, *attribute;
    while ((attribute = strsep(&state, ",")) != NULL) {
        if (attribute[0] == 'T') {
            return (const char *)[[NSData dataWithBytes:(attribute + 3) length:strlen(attribute) - 4] bytes];
        }
    }
    return "@";
}

@interface ClassMap : NSObject
+ (id) mapForClass:(Class)_class;

- (void) fillColumns:(Class)_class;
- (void) buildQueries;

- (id) getValueForColumn:(NSString *)_column withInstance:(id)instance;
- (id) getInstanceFromResultSet:(FMResultSet *)_rs;
- (id) getValueForPrimaryKeyWithInstance:(id)instance;
- (NSArray *) getValuesForEntity:(id)instance;
- (NSArray *) getValuesForEntity:(id)instance withPrimaryKeyLast:(BOOL)_primaryKeyLast;

@property (nonatomic, retain) Class class;
@property (nonatomic, retain) NSString * tableName;
@property (nonatomic, retain) NSNumber * version;
@property (nonatomic, retain) NSString * primaryKey;
@property (nonatomic, retain) NSArray * compositeKeyColumns;
@property (nonatomic, retain) NSArray * transientColumns;
@property (nonatomic, retain) NSDictionary * columns;
@property (nonatomic, retain) NSString * createQuery;
@property (nonatomic, retain) NSString * insertQuery;
@property (nonatomic, retain) NSString * selectQueryPrefix;
@property (nonatomic, retain) NSString * selectQuery;
@property (nonatomic, retain) NSString * replaceQuery;
@property (nonatomic, retain) NSString * updateQuery;
@property (nonatomic, retain) NSString * deleteQuery;
@property (nonatomic, retain) NSString * deleteBulkQueryFmtStr;

@end

@implementation ClassMap

@synthesize class, 
    tableName, 
    primaryKey, 
    compositeKeyColumns,
    version, 
    transientColumns,
    columns, 
    createQuery, 
    insertQuery,
    replaceQuery, 
    updateQuery, 
    deleteQuery, 
    deleteBulkQueryFmtStr, 
    selectQuery, 
    selectQueryPrefix;

+ (void) initialize {
	databaseTypeMap = [[NSMutableDictionary dictionaryWithObjectsAndKeys:
					    @"varchar", @"NSMutableString", 
						@"varchar", @"NSString", 
					    @"integer", @"NSInteger", 
					    @"integer", @"NSNumber", 
					    @"integer", @"NSDate", 
					    @"blob", @"NSDictionary", 
					    @"blob", @"NSMutableDictionary", 
					    @"blob", @"NSArray", 
					    @"blob", @"NSMutableArray",
						nil
					   ] retain];
}

+ (id) mapForClass:(Class)_class {
	ClassMap * map = [[[ClassMap alloc] init] autorelease];
	[map fillColumns:_class];
	
	map.class = _class;
	map.primaryKey = NSStringFromSelector([_class primaryKeyColumn]);

	if ([_class respondsToSelector:@selector(compositeKeyColumns)]) {
		NSMutableArray * composites = [NSMutableArray array];
		NSArray * cols = [_class compositeKeyColumns];
		for (NSValue * col in cols) {
			[composites addObject:NSStringFromSelector((SEL)[col pointerValue])];
		}
		map.compositeKeyColumns = composites;
	} else {
		map.compositeKeyColumns = [NSArray arrayWithObject:map.primaryKey];
	}

	if ([_class respondsToSelector:@selector(transientProperties)]) {
		map.transientColumns = [_class transientProperties];
	} else {
		map.transientColumns = [NSArray array];
	}

	map.version = [NSNumber numberWithInt:[_class version]];
	map.tableName = NSStringFromClass(_class);
	[map buildQueries];
	
	return map;
}

- (void) fillColumns:(Class)_class {
	unsigned int outCount, i;
	objc_property_t *properties = class_copyPropertyList(_class, &outCount);
	NSMutableDictionary * dict = [NSMutableDictionary dictionaryWithCapacity:outCount];
	for (i = 0; i < outCount; i++) {
		objc_property_t property = properties[i];
		const char * name = property_getName(property);
		const char * type = getPropertyType(property);
		NSString * propName = [NSString stringWithCString:name];
		NSString * propType = [NSString stringWithCString:type];
		
		[dict setObject:propType forKey:propName];
	}
	free(properties);
	self.columns = dict;
}

- (void) buildQueries {
	NSMutableArray * createParamsArray = [NSMutableArray arrayWithCapacity:[self.columns count]];
	NSMutableArray * updateParamsArray = [NSMutableArray arrayWithCapacity:[self.columns count]];
	NSMutableArray * insertParamsArray = [NSMutableArray arrayWithCapacity:[self.columns count]];
	
	for (NSString * col in [self.columns keyEnumerator]) {
		NSString * fieldDef = [NSString stringWithFormat:@"%@ %@", col, [databaseTypeMap objectForKey:[self.columns objectForKey:col]]];
		NSString * setPart = [NSString stringWithFormat:@"%@ = ?", col];
		
		[updateParamsArray addObject:setPart];
		[createParamsArray addObject:fieldDef];
		[insertParamsArray addObject:@"?"]; 
	}

	NSString * columnsStr = [[[self.columns keyEnumerator] allObjects] componentsJoinedByString:@", "];
	NSString * createParams = [createParamsArray componentsJoinedByString:@", "];
	NSString * insertParams = [insertParamsArray componentsJoinedByString:@", "];
	NSString * updateParams = [updateParamsArray componentsJoinedByString:@", "];
	NSString * createPrimaryKey = [NSString stringWithFormat:@" PRIMARY KEY (%@)", [self.compositeKeyColumns componentsJoinedByString:@", "]];

	self.createQuery = [NSString stringWithFormat:@"CREATE TABLE IF NOT EXISTS %@ (%@, %@)", self.tableName, createParams, createPrimaryKey];
	self.insertQuery = [NSString stringWithFormat:@"INSERT INTO %@ (%@) VALUES (%@)", self.tableName, columnsStr, insertParams];
	self.replaceQuery = [NSString stringWithFormat:@"REPLACE INTO %@ (%@) VALUES (%@)", self.tableName, columnsStr, insertParams];
	self.updateQuery = [NSString stringWithFormat:@"UPDATE %@ SET %@ WHERE %@ = ?", self.tableName, updateParams, self.primaryKey];
	self.deleteQuery = [NSString stringWithFormat:@"DELETE FROM %@ WHERE %@ = ?", self.tableName, self.primaryKey];
	self.deleteBulkQueryFmtStr = [NSString stringWithFormat:@"DELETE FROM %@ WHERE %@ IN (%%@)", self.tableName, self.primaryKey];
	self.selectQueryPrefix = [NSString stringWithFormat:@"SELECT %@ FROM %@ WHERE ", columnsStr, self.tableName];	
	self.selectQuery = [NSString stringWithFormat:@"%@ %@ = ?", self.selectQueryPrefix, self.primaryKey];	
}

- (NSArray *) getValuesForEntity:(id)instance {
	return [self getValuesForEntity:instance withPrimaryKeyLast:NO];
}

- (id) getValueForColumn:(NSString *)_column withInstance:(id)instance {
	id value = [instance performSelector:NSSelectorFromString(_column)];

	if ([@"blob" isEqualToString:[databaseTypeMap objectForKey:[self.columns objectForKey:_column]]]) {
		NSString * error = nil;
		value = [NSPropertyListSerialization dataFromPropertyList:value format:NSPropertyListBinaryFormat_v1_0 errorDescription:&error];
		[error release];
	}

	if (value == nil) {
		value = [NSNull null];
	}
	
	return value;
}

- (id) getInstanceFromResultSet:(FMResultSet *)_rs {
	id instance = [[[self.class alloc] init] autorelease];

	int count = [_rs columnCount];
	for (int idx = 0; idx < count; idx++) {
        NSString *column = [_rs columnNameForIndex:idx];
		id result = [_rs objectForColumnIndex:idx];
		if (result != nil) {
			if ([result isKindOfClass:[NSData class]]) {
				NSString * error = nil;
				NSPropertyListFormat format = NSPropertyListBinaryFormat_v1_0;
				result = [NSPropertyListSerialization propertyListFromData:result
														  mutabilityOption:NSPropertyListMutableContainers 
																	format:&format
														  errorDescription:&error];
				[error release];
			} else if ([[self.columns objectForKey:column] isEqualToString:@"NSDate"]) {
				result = [_rs dateForColumn:column];
			}
		}
		[instance setValue:result forKey:column];
	}
	/*
	NSDictionary * results = [_rs resultDict];
	for (NSString * column in [results keyEnumerator]) {
		id result = [results objectForKey:column];
		if (result != nil) {
			if ([result isKindOfClass:[NSData class]]) {
				NSString * error = nil;
				NSPropertyListFormat format = NSPropertyListBinaryFormat_v1_0;
				result = [NSPropertyListSerialization propertyListFromData:result
														  mutabilityOption:NSPropertyListMutableContainers 
																	format:&format
														  errorDescription:&error];
				[error release];
			} else if ([[self.columns objectForKey:column] isEqualToString:@"NSDate"]) {
				result = [_rs dateForColumn:column];
			}
		}
		[instance setValue:result forKey:column];
	}
	*/
	return instance;
}

- (id) getValueForPrimaryKeyWithInstance:(id)instance {
	return [self getValueForColumn:self.primaryKey withInstance:instance];
}

- (NSArray *) getValuesForEntity:(id)instance withPrimaryKeyLast:(BOOL)_primaryKeyLast
{
	NSMutableArray * values = [NSMutableArray array];
	
	for (NSString * column in [self.columns keyEnumerator]) {
//		if (_primaryKeyLast && [column isEqualToString:self.primaryKey])
//			continue;
		
		[values addObject:[self getValueForColumn:column withInstance:instance]];
	}
	
	if (_primaryKeyLast) {
		[values addObject:[self getValueForColumn:self.primaryKey withInstance:instance]];
	}
	
	return values;
}

@end

@interface EntityManager (private)
- (id) initWithDatabasePath:(NSString *)_path delegate:(id <EntityManagerDelegate>)_delegate;
//- (id) getInstanceOfClass:(ClassMap *)_classMap fromResultSet:(FMResultSet *)_rs;
- (void) initializeAndUpdateTables:(id <EntityManagerDelegate>)_delegate;
@end


@implementation EntityManager

@synthesize db;

+ (void) initialize {
	structureMap = [[NSMutableDictionary alloc] initWithCapacity:2];
}

+ (EntityManager *) openManagerWithDatabase:(NSString *)databasePath delegate:(id<EntityManagerDelegate>)_delegate {
	NSArray *paths = NSSearchPathForDirectoriesInDomains(NSDocumentDirectory, NSUserDomainMask, YES);
    NSString *dataPath = [[paths objectAtIndex:0] stringByAppendingPathComponent:databasePath];
	
	EntityManager * manager = [[[EntityManager alloc] initWithDatabasePath:dataPath delegate:_delegate] autorelease];
	
	return manager;
}

- (id) initWithDatabasePath:(NSString *)_path delegate:(id<EntityManagerDelegate>)_delegate {
	if (self == [super init]) {
		db = [[FMDatabase databaseWithPath:_path] retain];
		
#if DEBUG
		[self.db setTraceExecution:YES];
#endif
		[self.db setShouldCacheStatements:YES];
		
		[self.db open];
		
		[self initializeAndUpdateTables:_delegate];
	}
	
	return self;
}

- (void) dealloc {
	[self.db close];
	[db release];
	
	[super dealloc];
}

- (void) closeManager {
	[self.db close];
}

+ (void) registerClass:(Class)_class {
	if (![_class conformsToProtocol:@protocol(EntityDescriptor)]) {
		NSLog(@"The supplied class %@ does not conform to the ActiveObjectDescriptor protocol.", NSStringFromClass(_class));
		[NSException raise:@"InvalidArgumentException" 
					format:@"The supplied class %@ does not conform to the ActiveObjectDescriptor protocol.", NSStringFromClass(_class)];
	}
	[structureMap setObject:[ClassMap mapForClass:_class] forKey:NSStringFromClass(_class)];
}

#define CREATE_VERSIONS_TABLE  @"CREATE TABLE IF NOT EXISTS __AO_Entity_Versions (entityName varchar primary key, entityVersion integer)"
#define SELECT_VERSION_OF_ENT  @"SELECT entityVersion FROM __AO_Entity_Versions WHERE entityName = ?"
#define INSERT_VERSION_OF_ENT  @"INSERT INTO __AO_Entity_Versions (entityVersion, entityName) VALUES (?, ?)"
#define UPDATE_VERSION_OF_ENT  @"UPDATE __AO_Entity_Versions SET entityVersion = ? WHERE entityName = ?"

- (void) initializeAndUpdateTables:(id<EntityManagerDelegate>)_delegate {
	[self.db executeUpdate:CREATE_VERSIONS_TABLE];
	for (ClassMap * classMap in [structureMap objectEnumerator]) {		
		// Schema update...
		Class class = classMap.class;
		NSNumber * currentVersion = [NSNumber numberWithInt:[self.db intForQuery:SELECT_VERSION_OF_ENT, classMap.tableName]];
		NSNumber * entityVersion = classMap.version;
		if (0 == [currentVersion intValue]) {
			[self.db executeUpdate:INSERT_VERSION_OF_ENT, classMap.version, classMap.tableName];
		} else if (![currentVersion isEqualToNumber:entityVersion]) {
			NSString * schemaUpdateQuery = [_delegate foundVersionMismatchOnEntity:class 
																		 dbVersion:currentVersion 
																	 entityVersion:entityVersion];					
			if (schemaUpdateQuery != nil) {
				[self.db executeUpdate:schemaUpdateQuery];
			}

			[self.db executeUpdate:UPDATE_VERSION_OF_ENT, classMap.version, classMap.tableName];
		}
		
		// Table creation..
		[self.db executeUpdate:classMap.createQuery];
	}
}

- (void) replace:(id)instance {
	ClassMap * classMap = [structureMap objectForKey:NSStringFromClass([instance class])];
	
	[self.db executeUpdate:classMap.replaceQuery withArgumentsInArray:[classMap getValuesForEntity:instance]];
}

- (void) create:(id)instance {
	ClassMap * classMap = [structureMap objectForKey:NSStringFromClass([instance class])];
	
	[self.db executeUpdate:classMap.insertQuery withArgumentsInArray:[classMap getValuesForEntity:instance]];
}

- (void) update:(id)instance {
	ClassMap * classMap = [structureMap objectForKey:NSStringFromClass([instance class])];
	
	[self.db executeUpdate:classMap.updateQuery withArgumentsInArray:[classMap getValuesForEntity:instance withPrimaryKeyLast:YES]];
}

- (void) remove:(id)instance {
	ClassMap * classMap = [structureMap objectForKey:NSStringFromClass([instance class])];
	
	[self.db executeUpdate:classMap.deleteQuery, [classMap getValueForPrimaryKeyWithInstance:instance]];
}

- (void) removeWithClass:(Class)_class andPrimaryKey:(id)key {
	ClassMap * classMap = [structureMap objectForKey:NSStringFromClass(_class)];
	
	[self.db executeUpdate:classMap.deleteQuery, key];
}

- (void) removeWithClass:(Class)_class andPrimaryKeyList:(NSArray *)keys {
	ClassMap * classMap = [structureMap objectForKey:NSStringFromClass(_class)];
	
	NSMutableArray * partsArray = [NSMutableArray arrayWithCapacity:[keys count]];
	for (int i = 0; i < [keys count]; i++) {
		[partsArray addObject:@"?"];
	}

	NSString * queryStr = [NSString stringWithFormat:classMap.deleteBulkQueryFmtStr, [partsArray componentsJoinedByString:@","]];
	
	[self.db executeUpdate:queryStr withArgumentsInArray:keys];
}

- (void) removeWithClass:(Class)_class andQuery:(id)query, ... {
	ClassMap * classMap = [structureMap objectForKey:NSStringFromClass(_class)];

	NSString * queryStr = [NSString stringWithFormat:@"DELETE FROM %@ WHERE %@", classMap.tableName, query];
	
    va_list args;
    va_start(args, query);

	[self.db executeUpdate:queryStr error:nil withArgumentsInArray:nil orVAList:args];
	
	va_end(args);
}

- (id) loadSingleWithClass:(Class)_class andQuery:(id)query, ... {
	ClassMap * classMap = [structureMap objectForKey:NSStringFromClass(_class)];
	
	NSString * queryStr = [NSString stringWithFormat:@"%@ %@", classMap.selectQueryPrefix, query];

    va_list args;
    va_start(args, query);
	
	FMResultSet * rs = [self.db executeQuery:queryStr withArgumentsInArray:nil orVAList:args];
	
	va_end(args);

	id instance = nil;
	
	if ([rs next]) {
		instance = [classMap getInstanceFromResultSet:rs];
	}
	
	[rs close];
	
	return instance;
}

- (NSArray *) loadArrayWithClass:(Class)_class andQuery:(NSString *)query, ... {
	ClassMap * classMap = [structureMap objectForKey:NSStringFromClass(_class)];

	NSString * queryStr = [NSString stringWithFormat:@"%@ %@", classMap.selectQueryPrefix, query];
	
	va_list args;
    va_start(args, query);
	
	FMResultSet * rs = [self.db executeQuery:queryStr withArgumentsInArray:nil orVAList:args];

	va_end(args);
	
	NSMutableArray * resultSet = [NSMutableArray array];
	while ([rs next]) {
		[resultSet addObject:[classMap getInstanceFromResultSet:rs]];
	}
	
	[rs close];
	
	return resultSet;
}

- (id) loadWithClass:(Class)_class andPrimaryKey:(id)key {
	ClassMap * classMap = [structureMap objectForKey:NSStringFromClass(_class)];
	
	FMResultSet * rs = [self.db executeQuery:classMap.selectQuery, key];
	
	id instance = nil;
	
	if ([rs next]) {
		instance = [classMap getInstanceFromResultSet:rs];
	}
	
	[rs close];
	
	return instance;
}

- (BOOL)hasEntityForClass:(Class)_class andQuery:(NSString *)query, ... {
	ClassMap * classMap = [structureMap objectForKey:NSStringFromClass(_class)];
	
	NSString * queryStr = [NSString stringWithFormat:@"SELECT 1 FROM %@ WHERE %@", classMap.tableName, query];
	
	va_list args;
    va_start(args, query);
	
	FMResultSet * rs = [self.db executeQuery:queryStr withArgumentsInArray:nil orVAList:args];
	
	va_end(args);
	
	BOOL hasRows = [rs next];
	
	[rs close];
	
	return hasRows;
}

@end
