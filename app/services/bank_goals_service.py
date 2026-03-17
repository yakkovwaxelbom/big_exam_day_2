import io
from datetime import datetime

from utils.DigitalHunter_map import plot_map_with_geometry
from utils.haversine import haversine_km
from dal.bank_goals_dal import MySqlDalBankGoals

class bankGoalsService:

    def __init__(self, dal_cls, conn):

        self._dal: MySqlDalBankGoals = dal_cls(conn)

    
    def get_shifting_quality_goals(self):
        return self._dal.get_shifting_quality_goals()

    def get_analysis_collection_sources(self):
        return self._dal.get_analysis_collection_sources()

        
    def get_optional_new_goals(self):
        return self._dal.get_optional_new_goals()
    

    def identifying_awakened_sleeping_cells(self):
        return self._dal.identifying_awakened_sleeping_cells()
    
    def get_lan_lot_by_entity_id_order_time(self, entity_id: str):
        cord = self._dal.get_lan_lot_by_entity_id_order_time(entity_id)

        lot = cord.get('reported_lat', [])
        lan = cord.get('reported_lon', [])

        buffer = io.BytesIO()

        plot_map_with_geometry([lan, lot], buffer)

        img_bit = buffer.getvalue()

        return img_bit
    

    def get_analyzing_escape_patterns_after_attack(self):

        res = []
        seen = set()

        records = self._dal.get_analyzing_escape_patterns_after_attack()

        for record in records:

            before_b_lat = record['before_b_lat']
            before_b_lon = record['before_b_lon']
            before_b_ts: datetime = record['before_b_ts']

            before_e_lat = record['before_e_lat']
            before_e_lon = record['before_e_lon']
            before_e_ts: datetime = record['before_e_ts']

            after_b_lat = record['after_b_lat']
            after_b_lon = record['after_b_lon']
            after_b_ts: datetime = record['after_b_ts']

            after_e_lat = record['after_e_lat']
            after_e_lon = record['after_e_lon']
            after_e_ts: datetime = record['after_e_ts']

            dis_before = haversine_km(before_b_lat, before_b_lon, before_e_lat, before_e_lon)
            total_time = (before_e_ts - before_b_ts).total_seconds()

            if total_time == 0: continue

            avg_speed_before =  (dis_before*1000)/total_time 

            dis_after = haversine_km(after_b_lat, after_b_lon, after_e_lat, after_e_lon)
            total_time = (after_e_ts - after_b_ts).total_seconds()
            
            if total_time == 0: continue

            avg_speed_after =(dis_after*1000)/total_time

            if avg_speed_after == 0 or avg_speed_before == 0: continue

            ratio = avg_speed_after/avg_speed_before

            if ratio > 1.5:

                sub_res = (record['entity_id'], avg_speed_before,
                           avg_speed_after, ratio*100)
                
                seen.add(sub_res)

        for s in seen:

            sub_res = {
                'entity_id': s[0],
                'avg_speed_before': s[1],
                'avg_speed_after': s[2],
                'percentage_change': s[3]
            }

            res.append(sub_res)

        return res


    def finding_friends_meetings(self):

        seen = set() 

        records = self._dal.finding_friends_meetings()
    
        for record in records:
            s1_entity_id = record['s1_entity_id']
            s2_entity_id = record['s2_entity_id']

            s1_reported_lat = record['s1_reported_lat']
            s2_reported_lat = record['s2_reported_lat']

            s1_reported_lon = record['s1_reported_lon']
            s2_reported_lon = record['s2_reported_lon']

            s1_timestamp = record['s1_timestamp']
            s2_timestamp = record['s2_timestamp']

            dis = haversine_km(
                s1_reported_lat, s1_reported_lon, s2_reported_lat, s2_reported_lon)
            
            if dis < 1:

                avg_timestamp = (s1_timestamp.timestamp() + s2_timestamp.timestamp())/2

                avg_time = datetime.fromtimestamp(avg_timestamp)

                sub_res = {"entity_id_1": s1_entity_id,
                           'entity_id_2': s2_entity_id,
                           'average_time': avg_time,
                           'distance': dis}
                
                seen.add(sub_res)
        
        return list(seen)
                

                




        




