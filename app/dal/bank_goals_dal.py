from mysql.connector import MySQLConnection
from contextlib import contextmanager
class MySqlDalBankGoals:

    def __init__(self, conn: MySQLConnection):
        self._conn = conn


    @contextmanager
    def _get_safe_cursor(self, dictionary=True):

        cursor = None

        try:
            cursor = self._conn.cursor(dictionary=dictionary)

            yield cursor

            self._conn.commit()

            cursor.close()

        except Exception as e:

            if cursor:
                cursor.close()

            if self._conn:
                self._conn.rollback()

            raise e
            
        


    def get_shifting_quality_goals(self):

        q = '''select entity_id, target_name, priority_level, movement_distance_km
            from targets
            where priority_level = 1 or priority_level = 2 or movement_distance_km >= 5;'''

        with self._get_safe_cursor() as cursor:
            cursor.execute(q)

            res = cursor.fetchall()

        return res


    def get_analysis_collection_sources(self):

        q = '''select signal_type, count(*) as `count`
            from intel_signals
            group by signal_type;'''
        
        with self._get_safe_cursor() as cursor:
            cursor.execute(q)

            res = cursor.fetchall()

        return res



    def get_optional_new_goals(self):


        q ='''select entity_id , sum(`count`) as `count`
            from (
                select entity_id, count(*) as `count`
                from attacks
                where entity_id like '%UNKNOWN%'
                group by entity_id

                union all

                select entity_id, count(*) as `count`
                from damage_assessments
                where entity_id like '%UNKNOWN%'
                group by entity_id

                union all

                select entity_id, count(*) as `count`
                from targets
                where entity_id like '%UNKNOWN%'
                group by entity_id

                union all

                select entity_id, count(*) as `count`
                from intel_signals
                where entity_id like '%UNKNOWN%' or priority_level = 99
                group by entity_id) as temp
                
            group by entity_id
            order by `count` desc
            limit 3'''

        with self._get_safe_cursor() as cursor:
            cursor.execute(q)

            res = cursor.fetchall()

        return res
    

    def identifying_awakened_sleeping_cells(self):

        q = '''with temp_intel_signals as (
            select entity_id, distance_from_last, case 
                            when hour(timestamp) >= 0 and  hour(timestamp) < 8 then date_sub(timestamp, interval 1 day) 
                            else timestamp 
                                            end as timestamp
            from intel_signals)

            select entity_id 
            from temp_intel_signals
            where entity_id in (select entity_id
                                from temp_intel_signals
                                where hour(timestamp) >= 8 and hour(timestamp) < 20
                                group by entity_id
                                having sum(distance_from_last) = 0)
                                
            and entity_id in (select entity_id
                            from temp_intel_signals
                            where hour(timestamp) < 8 or hour(timestamp) >= 20
                            group by entity_id
                            having sum(distance_from_last) >= 10);'''
        
        with self._get_safe_cursor() as cursor:
            cursor.execute(q)
            res = cursor.fetchall()

        return res


    def get_lan_lot_by_entity_id_order_time(self, entity_id):
        
        q ='''select reported_lat, reported_lon
            from intel_signals
            where entity_id = %s
            order by timestamp;
            '''
        
        with self._get_safe_cursor() as cursor:
            cursor.execute(q, (entity_id,))
            cord = cursor.fetchall()

        res = {'reported_lat': [val['reported_lat'] for val in cord],
               'reported_lon': [val['reported_lon'] for val in cord]}

        return res


    def get_analyzing_escape_patterns_after_attack(self):

        q = '''with temp_entity_targets AS (
            select i.entity_id AS entity_id,
                i.timestamp AS ts,
                i.reported_lat,
                i.reported_lon
            from intel_signals i
            inner join targets t 
                on t.entity_id = i.entity_id 
            and t.status <> 'destroyed'
        ),
        temp_attack_intel_before_3 AS (
            select i.entity_id AS entity_id,
                a.attack_id,
                i.ts,
                i.reported_lat,
                i.reported_lon
            from attacks a
            inner join temp_entity_targets i 
                on i.entity_id = a.entity_id 
            and i.ts between date_sub(a.timestamp, interval 3 hour) 
                        and a.timestamp
        ),
        temp_attack_intel_after_3 AS (
            select i.entity_id AS entity_id,
                a.attack_id,
                i.ts,
                i.reported_lat,
                i.reported_lon
            from attacks a
            inner join temp_entity_targets i 
                on i.entity_id = a.entity_id 
            and i.ts between a.timestamp and date_add(a.timestamp, interval 3 hour)
        ),
        temp_attack_intel_before_3_begin as (
            select * 
            from temp_attack_intel_before_3 t1
            where t1.ts = (
                select min(t2.ts)
                from temp_attack_intel_before_3 t2
                where t1.entity_id = t2.entity_id 
                and t1.attack_id = t2.attack_id
            )
        ),
        temp_attack_intel_before_3_end as (
            select * 
            from temp_attack_intel_before_3 t1
            where t1.ts = (
                select max(t2.ts)
                from temp_attack_intel_before_3 t2
                where t1.entity_id = t2.entity_id 
                and t1.attack_id = t2.attack_id
            )
        ),
        temp_attack_intel_after_3_begin as (
            select * 
            from temp_attack_intel_after_3 t1
            where t1.ts = (
                select min(t2.ts)
                from temp_attack_intel_after_3 t2
                where t1.entity_id = t2.entity_id 
                and t1.attack_id = t2.attack_id
            )
        ),
        temp_attack_intel_after_3_end as (
            select * 
            from temp_attack_intel_after_3 t1
            where t1.ts = (
                select max(t2.ts)
                from temp_attack_intel_after_3 t2
                where t1.entity_id = t2.entity_id 
                and t1.attack_id = t2.attack_id
            )
        )

        select 
            t1.attack_id as attack_id, 
            t1.entity_id as entity_id, 
            t1.ts as after_b_ts,
            t1.reported_lat as after_b_lat,
            t1.reported_lon as after_b_lon,
            t2.ts as after_e_ts,
            t2.reported_lat as after_e_lat,
            t2.reported_lon as after_e_lon,
            t3.ts as before_b_ts,
            t3.reported_lat as before_b_lat,
            t3.reported_lon as before_b_lon,
            t34.ts as before_e_ts,
            t34.reported_lat as before_e_lat,
            t34.reported_lon as before_e_lon
        from temp_attack_intel_after_3_begin t1 
        inner join temp_attack_intel_after_3_end t2 
            on t1.entity_id = t2.entity_id and t1.attack_id = t2.attack_id
        inner join temp_attack_intel_before_3_begin t3 
            on t1.entity_id = t3.entity_id and t1.attack_id = t3.attack_id
        inner join temp_attack_intel_before_3_end t34
            on t1.entity_id = t34.entity_id and t1.attack_id = t34.attack_id'''


        with self._get_safe_cursor() as cursor:
            cursor.execute(q)
            res = cursor.fetchall()
        
        return res
        

    def finding_friends_meetings(self):

        q = """select s1.entity_id as s1_entity_id, 
            s2.entity_id as s2_entity_id, 
            s1.reported_lat as s1_reported_lat, 
            s2.reported_lat as s2_reported_lat,
            s1.reported_lon as s1_reported_lon, 
            s2.reported_lon as s2_reported_lon, 
            s1.timestamp as s1_timestamp, 
            s2.timestamp as s2_timestamp
       
            from intel_signals s1, intel_signals s2
            where s1.entity_id <> s2.entity_id 
            and s2.timestamp between s1.timestamp and date_add(s1.timestamp, interval 10 minute)
            """

        with self._get_safe_cursor() as cursor:
            cursor.execute(q)
            res = cursor.fetchall()
        
        return res
        