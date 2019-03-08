from pydicom.dataset import Dataset
from pydicom.sequence import Sequence
from pynetdicom import AE, BasicWorklistManagementPresentationContexts

import mysql.connector


class ModalityWorkList:
    ae = AE()
    config = {}
    debug = False

    def __init__(self, aet, user_db, password_db, ip_db, name_db, debug):
        # Set database access
        self.config = {
            'user': user_db,
            'password': password_db,
            'host': ip_db,
            'database': name_db,
            'raise_on_warnings': True
        }
        # Set dicom config
        self.ae.ae_title = aet
        self.ae.require_called_aet = True
        self.ae.supported_contexts = BasicWorklistManagementPresentationContexts
        self.ae.on_c_find = self.on_c_find
        # Set debug
        self.debug = debug

    def execute(self, port):
        try:
            print("Started MWL service on port ", port)
            self.ae.start_server(('', port), block=True)
        except Exception as e:
            print("Error on start MWL : ", str(e))

    def on_c_find(self, dataset, context, info):
        if self.debug:
            print("MWL Requestor : ", info['requestor'])
            print("MWL query : ", dataset)
        try:
            scheduled_procedure = dataset.ScheduledProcedureStepSequence
            try:
                where_aetitle = " and sps_station_aet.station_aet = '{}' ".format(
                    scheduled_procedure[0].ScheduledStationAETitle)
            except:
                where_aetitle = ''
            try:
                date_filter = scheduled_procedure[0].ScheduledProcedureStepStartDate
                if len(date_filter) > 1 and '-' in date_filter:
                    if date_filter.startswith('-'):
                        where_start_date = " and date(mwl_item.sps_start_date) <= date('{}') ".format(date_filter[1:])
                    elif date_filter.endswith('-'):
                        where_start_date = " and date(mwl_item.sps_start_date) >= date('{}') ".format(date_filter[:-1])
                    else:
                        start_date, end_date = date_filter.split('-')
                        where_start_date = " and date(mwl_item.sps_start_date) between date('{}') and date('{}') ".format(
                            start_date, end_date)
                elif len(date_filter) > 1:
                    where_start_date = " and mwl_item.sps_start_date = '{}' ".format(date_filter)
                else:
                    where_start_date = ''
            except:
                where_start_date = ''
            try:
                time_filter = scheduled_procedure[0].ScheduledProcedureStepStartTime
                if len(time_filter) > 1 and '-' in time_filter:
                    if time_filter.startswith('-'):
                        where_start_time = " and time(mwl_item.sps_start_time) <= time('{}59.999') ".format(
                            time_filter[1:][:4])
                    elif time_filter.endswith('-'):
                        where_start_time = " and time(mwl_item.sps_start_time) >= time('{}00.000') ".format(
                            time_filter[:-1][:4])
                    else:
                        start_time, end_time = time_filter.split('-')
                        where_start_time = " and time(mwl_item.sps_start_time) between time('{}00.000') and time('{}59.999') ".format(
                            start_time[:4], end_time[:4])
                elif len(time_filter) > 1:
                    where_start_time = " and concat(substr(mwl_item.sps_start_time,1,4), '00') = '{}00' ".format(
                        time_filter[:4])
                else:
                    where_start_time = ''
            except:
                where_start_time = ''
            try:
                where_modality = " and mwl_item.modality = '{}' ".format(scheduled_procedure[0].Modality)
            except:
                where_modality = ''
        except:
            where_aetitle = ''
            where_start_date = ''
            where_start_time = ''
            where_modality = ''
        try:
            patient_name = dataset.PatientName
            where_patient_name = " and person_name.family_name = '{}' and person_name.given_name = '{}'".format(
                patient_name.family_name, patient_name.given_name)
        except:
            where_patient_name = ''
        try:
            where_patient_id = " and patient_id.pat_id = '{}'".format(dataset.PatientID)
        except:
            where_patient_id = ''

        cnx = mysql.connector.connect(**self.config)
        cursor = cnx.cursor()
        str_sql = """
        SELECT study.study_desc, mwl_item.accession_no, mwl_item.modality, mwl_item.sps_start_date, mwl_item.sps_start_time, 
        mwl_item.study_iuid, patient_id.pat_id, patient.pat_birthdate, patient.pat_sex, person_name.family_name, 
        person_name.given_name
        FROM mwl_item
        inner join study on study.study_iuid = mwl_item.study_iuid
        inner join patient on patient.pk = mwl_item.patient_fk
        inner join patient_id on patient_id.pk = patient.patient_id_fk
        inner join person_name on person_name.pk = patient.pat_name_fk
        left join sps_station_aet on sps_station_aet.mwl_item_fk = mwl_item.pk
        where	study.access_control_id = %s
        and		mwl_item.sps_status = 1
        """
        str_sql = str_sql + where_aetitle
        str_sql = str_sql + where_start_date
        str_sql = str_sql + where_start_time
        str_sql = str_sql + where_modality
        str_sql = str_sql + where_patient_name
        str_sql = str_sql + where_patient_id
        str_where = (self.ae.ae_title.decode('utf-8').strip(),)
        cursor.execute(str_sql, str_where)
        rows = cursor.fetchall()
        if self.debug:
            print("StrSQL : ", cursor.statement)
            print("RowCount : ", cursor.rowcount)
        if cursor.rowcount > 0:
            for row in rows:
                item = Dataset()
                item.add_new(0x00080005, 'CS', 'ISO_IR 100')
                item.add_new(0x00080050, 'SH', row[1])
                item.add_new(0x00100020, 'LO', row[6])
                item.add_new(0x00100010, 'PN', '{}^{}'.format(row[9].replace('>', ' '), row[10]))
                item.add_new(0x00100030, 'DA', row[7])
                item.add_new(0x00100040, 'CS', row[8])
                item.add_new(0x0020000D, 'UI', row[5])
                seq = Sequence()
                item_seq = Dataset()
                item_seq.add_new(0x00080060, 'CS', row[2])
                item_seq.add_new(0x00400002, 'DA', row[3])
                item_seq.add_new(0x00400003, 'TM', row[4])
                item_seq.add_new(0x00400007, 'LO', row[0])
                seq.append(item_seq)
                item.add_new(0x00400100, 'SQ', seq)
                if self.debug:
                    print("MWL RSP item : ", item)
                yield (0xFF00, item)
            cursor.close()
            cnx.close()
        else:
            cursor.close()
            cnx.close()
            return 0x0000

    def check_database_connection(self):
        database = False
        try:
            cnx = mysql.connector.connect(**self.config)
            cursor = cnx.cursor()
            str_sql = "select now()"
            cursor.execute(str_sql)
            rows = cursor.fetchall()
            if cursor.rowcount > 0:
                print("Database connection established at ", rows[0][0])
                database = True
            else:
                database = False
            cursor.close()
            cnx.close()
        except Exception as e:
            print("Error on connection DB : ", str(e))
        return database
