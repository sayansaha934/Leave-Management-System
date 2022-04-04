from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import pandas as pd
from datetime import datetime, timedelta


class cassandra_operation:
    def __init__(self):
        self.tableName='db_operation.employee'
        self.zipFile='secure-connect-leave-management.zip'
        self.column=['id','emp_id','name', 'email_id', 'contact_no', 'password', 'd_o_b', 'joindate', 'position', 'salary', 'month_leave', 'total_leaves', 'leave_date', 'leave_type', 'leave_status']
    def get_session(self):
        '''

        :return: session
        '''
        try:
            cloud_config = {
                'secure_connect_bundle': self.zipFile
            }
            auth_provider = PlainTextAuthProvider('UKTuZFCtauYBubKfsgTWtGSg', 'ZZH0ATDSD_-.GdSc9EePiyEZxr-yu0XoDD7k04.474W+1IoNjrf-QsYeoHlj7KXk06,QunWxZRi.3oEX0XyaY2YG6.hnMppTHtiH6OyBHu4mRSRsC627Kv9qpPXbWIcs')
            cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
            session = cluster.connect()
            return  session
        except Exception as e:
            raise e



    def login(self, email_id, password):
        '''

        :return: dictionary containing position and name
        '''
        try:
            session=self.get_session()
            query=f"SELECT position, name FROM {self.tableName} WHERE email_id='{email_id}' AND password='{password}' ALLOW FILTERING;"
            row=session.execute(query).one()
            if row is None:
                return None
            else:
                output={'position':row[0], 'name':row[1]}
                return output
        except Exception as e:
            raise e



    def get_last_id(self):
        '''
        :return: last id(PRIMARY KEY) of table
        '''
        try:
            session=self.get_session()
            query=f"SELECT id FROM {self.tableName};"
            row=session.execute(query)
            last_id=max(row)[0]
            return last_id
        except Exception as e:
            raise e


    def Add_One_Employee(self, info_list):
        '''
        :return: None

        It takes a list of one employee's information and add this in database
        '''
        try:
            i=self.column
            j=info_list
            query=f"INSERT INTO {self.tableName} ({i[0]},{i[1]},{i[2]},{i[3]},{i[4]},{i[5]},{i[6]},{i[7]},{i[8]},{i[9]}, {i[10]}, {i[11]}) VALUES ({j[0]},'{j[1]}','{j[2]}', '{j[3]}','{j[4]}','{j[5]}','{j[6]}','{j[7]}','{j[8]}',{j[9]}, {j[10]}, {j[11]});"
            session=self.get_session()
            session.execute(query)
        except Exception as e:
            raise e


    def Add_Many_Employee(self, path):
        '''

        :return:None

        It takes a file path of employees details and add all  in database
        '''
        try:
            session=self.get_session()
            i=self.column
            id = int(self.get_last_id()) + 1
            df=pd.read_csv(path)
            for idx,row in df.iterrows():
                row=list(row)
                emp_id='iNeuron'+str(id)
                password=row[0].split()[0]+"@"+row[3].split('-')[-1]
                j=[id, emp_id]+row[:3]+[password]+row[3:]+[2.0, 24.0]  #final list of employee info
                query = f"INSERT INTO {self.tableName} ({i[0]},{i[1]},{i[2]},{i[3]},{i[4]},{i[5]},{i[6]},{i[7]},{i[8]},{i[9]}, {i[10]}, {i[11]}) VALUES ({j[0]},'{j[1]}','{j[2]}', '{j[3]}','{j[4]}','{j[5]}','{j[6]}','{j[7]}','{j[8]}',{j[9]}, {j[10]}, {j[11]});"
                session.execute(query)
                id+=1
        except Exception as e:
            raise e


    def deleteEmployee(self, email_id):
        '''

        :return: True if employee deleted successfully or False if there is no employee with given email id
        '''
        try:
            session=self.get_session()
            query1=f"SELECT id FROM {self.tableName} WHERE email_id='{email_id}' ALLOW FILTERING;"
            row=session.execute(query1).one()
            if row==None:
                return False
            else:
                id=row[0]
                query2=f'DELETE FROM {self.tableName} WHERE id={id} IF EXISTS;'
                session.execute(query2)
                return True
        except Exception as e:
            raise e


    def getInfo(self, email_id):
        '''

        :return: dictionary of employee info or None if no employee with given email id
        '''
        try:
            session=self.get_session()
            query=f"SELECT emp_id, name, email_id, contact_no, d_o_b, joindate, position, salary FROM {self.tableName} WHERE email_id='{email_id}' ALLOW FILTERING;"
            row=session.execute(query).one()
            if row==None:
                return None
            else:
                id=row[0]
                info={'emp_id':row[0], 'name':row[1], 'email_id':row[2], 'contact_no':row[3], 'dob':row[4], 'join_date':row[5], 'position':row[6], 'salary':row[7]}
                return info
        except Exception as e:
            raise e


    def updateEmployeeInfo(self, update_value_list):
        '''

        :return: None

        updates employees info with new data in database
        '''
        try:
            session=self.get_session()
            j=update_value_list
            query1=f"SELECT id FROM {self.tableName} WHERE email_id='{j[1]}' ALLOW FILTERING;"
            id=session.execute(query1).one()[0]
            query2=f"UPDATE {self.tableName} SET name='{j[0]}', contact_no='{j[2]}', d_o_b='{j[3]}', joindate='{j[4]}', position='{j[5]}', salary= {j[6]} WHERE id={id};"
            session.execute(query2)
        except Exception as e:
            raise e


    def applyLeave(self, email_id, leave_date, leave_type, leave_status):
        '''
        :return: None

        It sets the leave date, leave type and leave status=Pending in database
        '''
        try:
            session=self.get_session()
            query1 = f"SELECT id FROM {self.tableName} WHERE email_id='{email_id}' ALLOW FILTERING;"
            id = session.execute(query1).one()[0]
            query2=f"UPDATE {self.tableName} SET leave_date='{leave_date}', leave_type={leave_type}, leave_status='{leave_status}' WHERE id={id};"
            session.execute(query2)
        except Exception as e:
            raise e


    def getPendingLeaves(self):
       '''

       :return: list of dictionaries containing emp_id, name, position, leave_date, leave_type where leave_status=Pending
       '''
       try:
           session=self.get_session()
           query=f"SELECT emp_id, name, position, leave_date, leave_type FROM {self.tableName} WHERE leave_status='Pending' ALLOW FILTERING;"
           leaves=session.execute(query)
           pending_leaves = []
           for i in leaves:
               mydict = {'emp_id': i[0], 'name': i[1], 'position': i[2], 'leave_date': i[3], 'leave_type': i[4]}
               pending_leaves.append(mydict)
           return pending_leaves
       except Exception as e:
           raise e

    def approveLeave(self, emp_id):
        '''
        :return: None
        sets leave_status = Approved
        '''
        try:
            session=self.get_session()
            query1 = f"SELECT id, month_leave, total_leaves, leave_type FROM {self.tableName} WHERE emp_id='{emp_id}' ALLOW FILTERING;"
            row = session.execute(query1).one()
            id = row[0]
            month_leave=row[1]-row[3]
            total_leaves=row[2]-row[3]
            query2=f"UPDATE {self.tableName} SET leave_status='Approved', month_leave={month_leave}, total_leaves={total_leaves}  WHERE id={id};"
            session.execute(query2)
        except Exception as e:
            raise e


    def rejectLeave(self, emp_id):
        '''

        :return: None
        sets leave_status=Rejected
        '''
        try:
            session=self.get_session()
            query1 = f"SELECT id FROM {self.tableName} WHERE emp_id='{emp_id}' ALLOW FILTERING;"
            row = session.execute(query1).one()
            id = row[0]
            query2=f"UPDATE {self.tableName} SET leave_status='Rejected' WHERE id={id};"
            session.execute(query2)
        except Exception as e:
            raise e


    def autoApproveLeave(self):
        '''

        :return: None
        Sets leave_status=Approved where leave_date is one day ahead of current date
        '''
        try:
            session=self.get_session()
            date_to_approve=datetime.date(datetime.today())+timedelta(days=1)
            query1=f"SELECT id, leave_type, total_leaves FROM {self.tableName} WHERE leave_date='{date_to_approve}' AND leave_status='Pending' ALLOW FILTERING;"
            rows=session.execute(query1)
            for row in rows:
                id=row[0]
                leave_type=row[1]
                total_leaves=row[2]-leave_type
                query2=f"UPDATE {self.tableName} SET leave_status='Approved', total_leaves={total_leaves} WHERE id={id};"
                session.execute(query2)
        except Exception as e:
            raise e


    def update_month_leave(self):
        '''

        :return:None
        sets month_leave=2.0
        '''
        try:
            session=self.get_session()
            query1=f"SELECT id FROM {self.tableName};"
            ids=session.execute(query1)
            for id in ids:
                query2=f"UPDATE {self.tableName} SET month_leave=2.0 WHERE id={id[0]};"
                session.execute(query2)
        except Exception as e:
            raise e


    def update_total_leaves(self):
        '''

        :return:None
        Sets total_leave = 24.0
        '''
        try:
            session = self.get_session()
            query1 = f"SELECT id FROM {self.tableName};"
            ids = session.execute(query1)
            for id in ids:
                query2 = f"UPDATE {self.tableName} SET total_leaves=24.0 WHERE id={id[0]};"
                session.execute(query2)
        except Exception as e:
            raise e


    def available_montly_leave(self, email_id):
        '''

        :return:No. of leaves left for current month
        '''
        try:
            session=self.get_session()
            query=f"SELECT month_leave FROM {self.tableName} WHERE email_id='{email_id}' ALLOW FILTERING;"
            leaves_left=session.execute(query).one()[0]
            return  leaves_left
        except Exception as e:
            raise e


    def update_cash(self):
        '''

        :return:None
        Sets cash=(Salary/30)*(total_leaves) where total_leaves>22.0
        '''
        try:
            session=self.get_session()
            query1 = f"SELECT id, salary, total_leaves FROM {self.tableName} WHERE total_leaves>22.0 ALLOW FILTERING;"
            row = session.execute(query1)
            for i in row:
                applicable_days=float(i[2])-22.0
                cash=applicable_days*(float(i[1])/30)
                query2=f"UPDATE {self.tableName} SET cash={cash} WHERE id={i[0]};"
                session.execute(query2)
        except Exception as e:
            raise e


    def get_leave_status(self, email_id):
        '''

        :return:[leave_status, leave_date, leave_type] or None if no emplyee with given email id
        '''
        try:
            session=self.get_session()
            query=f"SELECT leave_status, leave_date, leave_type FROM {self.tableName} WHERE email_id='{email_id}' ALLOW FILTERING;"
            row=session.execute(query).one()
            if row[0]==None or row[1]==None or row[2]==None:
                return None
            else:
                leave_status=row[0]
                leave_date=row[1]
                leave_type=row[2]
                return [leave_status, leave_date, leave_type]
        except Exception as e:
            raise e


    def get_money(self, email_id):
        '''

        :return: amount available or None if no amount is available
        '''
        try:
            session=self.get_session()
            query1=f"SELECT cash, id FROM {self.tableName} WHERE email_id='{email_id}' ALLOW FILTERING;"
            row=session.execute(query1).one()
            money=row[0]
            if money==None or money==0:
                return None
            else:
                id=row[1]
                query2=f"UPDATE {self.tableName} SET cash=0.0 WHERE id={id};"
                session.execute(query2)
                return money
        except Exception as e:
            raise e







