# coding: utf-8
'''
Created by VincentLee on 2015/7/24.
'''
import cx_Oracle
import csv

class in_oracle():
    #初始化函数，输入数据库连接，文件路径，表名称
    def __init__(self,con_str,file_path,table_name):
        self.con_str=con_str
        self.file_path=file_path
        self.table_name=table_name
    #根据表头建表
    def create_table(self,list):
        conn = cx_Oracle.connect(self.con_str)
        cursor = conn.cursor()
        fields = [i+' varchar2(500)' for i in list]
        fields_str = ', '.join(fields)
        sql = 'create table %s (%s)' % (self.table_name, fields_str)
        print sql
        try:
            #如果表存在，则先删除
            delete_sql='drop table '+self.table_name+' purge'
            cursor.execute(delete_sql)
        except Exception,e:
            print 'ERROR:',e
        finally:
            #创建表
            cursor.execute(sql)
            print 'create table '+ self.table_name+' success'
        cursor.close()
        conn.commit()
        print 'create table success'
        conn.close()

    #根据第一行建表，因此读取的第一行必须是表头
    def get_title(self):
        with open(self.file_path, 'rb') as f:
            reader = csv.reader(f)
            # contents = [i for i in reader]
            j=0
            for i in reader:
                if j==0:
                    self.title=i
                    return i

    #向表中插入数据，把第一行表头跳过，然后开始插入数据，每次插入20000条，则提交一次。
    def insert_date(self):
        conn = cx_Oracle.connect(self.con_str)
        cursor = conn.cursor()
        with open(self.file_path, 'rb') as f:
            reader = csv.reader(f)
            j=0
            content=[]
            for line in reader:
                if j==0:
                    j=j+1
                    continue
                j=j+1
                content.append(tuple(line))
                if j%40000==0:
                    a = [':%s' %i for i in range(len(self.get_title())+1)]
                    value= ','.join(a[1:])
                    sql = 'insert into %s values(%s)' %(self.table_name, value)
                    cursor.executemany(sql,content)
                    conn.commit()
                    content=[]
                    conn.commit()
                    print str(j-1)+'行已经插入'
                # if j%20000==0:
                #     conn.commit()
                #     print str(j-1)+'行已经插入'
            a = [':%s' %i for i in range(len(self.get_title())+1)]
            value= ','.join(a[1:])
            sql = 'insert into %s values(%s)' %(self.table_name, value)
            cursor.executemany(sql,content)
            conn.commit()
            conn.commit()
            print str(j-1)+'行已经插入'

            cursor.close()
            conn.commit()
            conn.close()
            print '数据插入完成'

if __name__=='__main__':
    con_str='BI/111111@ORCL'
    file_path='F:\\ordrs.csv'
    table_name='u_ordrs'
    io=in_oracle(con_str,file_path,table_name) #创建连接数据库的实例
    io.create_table(io.get_title()) #建表
    io.insert_date() #插入数据
