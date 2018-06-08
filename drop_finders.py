#!/opt/cloudera/parcels/Anaconda-4.0.0/bin/python

class DropFinder:
    
    def __init__(self, drops, drop_users_part = 0.1, max_users = 30):
        self.drops = drops
        self.drop_users_part = drop_users_part
        self.loaded = None
        self.result = None
        self.range_loaded = None
        self.max_users = max_users

    def get_drops_by_day(self, sqlContext, day, loaded = None):
        
        loaded = loaded if loaded is not None else self.load_from_hive(sqlContext, day)
        filtered = filter_by_user_count(loaded, self.drops, self.drop_users_part, self.max_users)
        with_source = add_source_column(filtered, self.drops)
        result = get_flattened_dataframe(with_source)

        self.result = result
        
        return result
    
    def get_drops_in_range(self, sqlContext, days):

        results = list()
        for day in days:
            self.loaded = None
            day_result = self.get_drops_by_day(sqlContext, day)
            results.append((day_result, day))

        self.range_loaded = results
    
        return results



class PhoneFinder(DropFinder):
    
    def load_from_hive(self, sqlContext, day, table="core_internal_rsa_pc.sbrfr_custom_fact", reload_hive=False):
        if self.loaded is not None and not reload_hive:
            return self.loaded
        
        from pyspark.sql.functions import col
        
        table = sqlContext.table("core_internal_rsa_pc.sbrfr_custom_fact").filter(col('short_date_str') == day)\
                    .select(['cdf_s_1', 'cdf_s_236']).na.drop()\
                    .toDF('phone', 'card_number')
                
        drop_phones = table.filter(col('card_number').isin(self.drops))\
                            .select(['phone']).distinct().toPandas().phone.tolist()
            
        result = table.filter(col('phone').isin(drop_phones))\
        .distinct()\
        .toPandas()
        self.loaded = result
        
        return result 


class FingerprintFinder(DropFinder):
    
    def load_from_hive(self, sqlContext, day, table="core_internal_rsa_pc.event_log", reload_hive=False):
        if self.loaded is not None and not reload_hive:
            return self.loaded
        
        from pyspark.sql.functions import col, udf, lit, concat
        #get_fields_from_ip = udf(lambda x: '.'.join(x.split('.')[:2]))
        
        table = sqlContext.table(table).filter(col('short_date_str') == day)\
                    .select(['data_s_1', 'ip_address', 'user_id']).na.drop()\
                    .withColumn('complex_index', concat(col('data_s_1'), lit('|'), col('ip_address')))\
                    .select(['complex_index', 'user_id'])
        drops_index = table.filter(col('user_id').isin(self.drops))\
                           .select(['complex_index']).distinct()    
        cmpl_idx = drops_index.toPandas().complex_index.values.tolist()
        
        result = table.filter(col('complex_index').isin(cmpl_idx))\
        .distinct()\
        .toPandas()
        
        result['complex_index'] += '|' + day
        
        self.loaded = result
        
        return result  
    

class HardwareFinder(DropFinder):    

    def load_from_hive(self, sqlContext, day, table="core_internal_rsa_pc.event_log", reload_hive=False):
        if self.loaded is not None and not reload_hive:
            return self.loaded
        
        from pyspark.sql.functions import col
    
        table = sqlContext.table(table)\
                .filter(col('short_date_str') == day)\
                .filter(col('data_s_34') != '-1')
        
        table = table.select(['data_s_34', 'user_id']).na.drop()
        drops_index = table.filter(col('user_id').isin(self.drops))\
        .select(['data_s_34']).distinct()
        
        hrdw_id_drops = drops_index.toPandas().data_s_34.values.tolist()
        
        result = table.toDF('hardware_id', 'user_id')\
        .filter(col('hardware_id').isin(hrdw_id_drops))\
        .distinct()\
        .toPandas()
        
        return result
    
def filter_by_user_count(df, drops, drop_users_part, max_users):
    if df.empty: return df
    
    index, source = df.columns
    grouped = df.groupby(index).agg({source: {
                                    'source_part' : lambda x: len(set(x) & set(drops))*1.0 / len(set(x)),
                                    'source_count': lambda x: len(set(x))}
                                    })[source].reset_index()
            
    index_filtered = grouped[index][(grouped['source_part'] > drop_users_part) & 
                                    (grouped['source_count'] <= max_users)]
    
    result = df[df[index].isin(index_filtered)].drop_duplicates()
    
    return result


def add_source_column(df, drops):
    if df.empty: return df
    
    index, source = df.columns
    idToSourse = df.groupby(index)\
                   .agg({source:  {'drops_source': lambda x: ','.join(set(x) & set(drops))}})\
                   [source].to_dict()['drops_source']
                
    df.loc[:,'drops_source'] = df[index].apply(lambda x: idToSourse[x])
    
    result = df[~df[source].isin(drops)]
    
    return result


def get_flattened_dataframe(df):
    from pandas import DataFrame, concat
    if df.empty: return df
    
    flattened = list()
    for row in df[df.drops_source.str.contains(',')].values:
        sources = row[2].split(',')
        n = len(sources)
        flattened_row = zip([row[0]]*n, [row[1]]*n, sources)
        flattened.extend(flattened_row)

    flattened = DataFrame(flattened, columns=df.columns)
    atomic_source = df[~df.drops_source.str.contains(',')]

    result = concat([atomic_source, flattened]).reset_index(drop=True)
    
    return result



#def filter_by_user_count(df, drops, drop_users_part):
#    index = df.columns[0]
#    grouped = df.groupby(index, as_index=False)\
#                .agg(lambda x: len(set(x) & set(drops))*1.0 / len(set(x)))
#            
#    index_filtered = grouped[index][grouped.user_id > drop_users_part]
#    
#    result = df[df[index].isin(index_filtered)].drop_duplicates()
#    
#    return result
#
#
#def add_source_column(df, drops):
#    index = df.columns[0]
#    idToSourse = df.groupby(index)\
#                   .agg({'user_id':  {'drops_source': lambda x: ','.join(set(x) & set(drops))}})\
#                   .user_id.to_dict()['drops_source']
#                
#    df.loc[:,'drops_source'] = df[index].apply(lambda x: idToSourse[x])
#    
#    result = df[~df.user_id.isin(drops)]
#    
#    return result
#
#
#def get_flattened_dataframe(df):
#    from pandas import DataFrame, concat
#    
#    flattened = list()
#    for row in df[df.drops_source.str.contains(',')].values:
#        sources = row[2].split(',')
#        n = len(sources)
#        flattened_row = zip([row[0]]*n, [row[1]]*n, sources)
#        flattened.extend(flattened_row)
#
#    flattened = DataFrame(flattened, columns=df.columns)
#    atomic_source = df[~df.drops_source.str.contains(',')]
#
#    result = concat([atomic_source, flattened]).reset_index(drop=True)
#    
#    return result