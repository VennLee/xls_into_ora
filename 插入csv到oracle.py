# coding: utf-8
# by liwenqiang 2016-08-27
''' 此代码用于将csv导入到oracle中。由于很多csv文件从数据库中导出的时候并不是使用逗号分隔的,
    如果使用excel将文档调整成使用逗号分隔的csv，如果数据量超过100W条的时候会将多余的数据漏掉。
    使用plsql工具导入的话，性能太低，导入太慢。所以使用python进行批量插入的方法很快。这个脚本
    将逗号分隔和非逗号分隔的csv都考虑进来了。用户可以根据具体的情况进行配置，以方便将数据快速
    准确的导入oracle中。
    运行此代码，需要安装 cx_Oracle库。运行的时候配置相关信息，然后运行即可。
'''
import csv
import cx_Oracle

class in_Oracle:
    #创建初始化函数，需要传入的参数有：
    # 1oracle数据库连接 2文件路径 3 表名 4 frstline_for_title，表示首行是否创建为标题，默认为1表示是，否则不是
    # 5 separator，表示是否逗号分隔的csv，默认为1表示是,否则不是 6 提交行数，默认为50000，可以设置
    def __init__(self, con_str, file_path, table_name,frstline_for_title=0,separator=1,commit_line=50000):
        self.con_str = con_str
        self.file_path = file_path
        self.table_name = table_name
        self.separator=separator
        self.frstline_for_title=frstline_for_title
        #以下为全局变量，为csv的列数,commit_line为提交的行数，是为了方便查看插入数据进度设置的
        self.column_count=0
        self.commit_line=commit_line
    # 读取csv函数,这里读取的是非逗号分隔的csv
    def get_csv1(self):
        #获取配置信息
        filepath=self.file_path
        frstline_for_title=self.frstline_for_title
        #获取标题
        title=[]
        #获取数据，data2用于返回。由于此函数是用于读取非逗号分隔的csv，也就是默认导出的csv
        #故需要对读取的数据进行处理
        data=[]
        data2=[]
        with open(filepath, 'rb') as f:
            reader = csv.reader(f)
            contents = [i for i in reader]
        #第一行为标题的情况
        if frstline_for_title==1:
            title=list(str(contents[0][0]).replace('\t', ',').split(','))
            data = contents[1:]
        else:
        #第一行为非标题的情况
            for i in range(len(list(str(contents[0][0]).replace('\t', ',').split(',')))):
                title.append('colunm' + str(i + 1))
            data=contents[0:]
        #title处理完成后，使用以下循环将data进行切片处理，变成多维数组
        for line in data:
            data2.append(list(str(line[0]).replace('\t',',').split(',')))
        self.column_count=len(title)
        print self.column_count
        return (title, data2)
    #此函数用于读取以逗号分隔的csv文件
    def get_csv2(self):
        filepath=self.file_path
        frstline_for_title=self.frstline_for_title
        title=[]
        data=[]
        with open(filepath, 'rb') as f:
            reader = csv.reader(f)
            contents = [i for i in reader]
        if frstline_for_title==1:
            #第一行是标题,先转换标题
            title = contents[0]
            data = contents[1:]
        else:
            #第一行不是标题，创建标题
            for i in range(len(contents[0])+1):
                title.append('column'+str(i))
            print title
            data=contents[0:]

        self.column_count = len(title)
        print self.column_count
        return (title, data)
    #通过全局变量 self.separator 来判断是那种格式的csv文件，调用不同的函数
    def get_csv(self):
        if self.separator==1:
            print '你需要导入的是逗号分隔的csv文件。'
            return self.get_csv2()
        else:
            print '你需要导入的是非逗号分隔的csv文件。'
            return self.get_csv1()
    #建表函数
    def create_table(self,title):
        try:
            conn = cx_Oracle.connect(self.con_str)
            cursor = conn.cursor()
        except Exception, e:
            print '数据库登录失败，请验证信息后重新尝试:\n'
            print e.message
        fields = [i + ' varchar2(800)' for i in title]
        fields_str = ', '.join(fields)
        #建表
        create_sql = 'create table %s (%s)' % (self.table_name, fields_str)
        print '建表语句为：\n'+create_sql.replace(',',',\n')
        try:
            #尝试删除表名，如果表已经存在即删除，否则建表
            delete_sql = 'drop table ' + self.table_name + ' purge'
            cursor.execute(delete_sql)
            print '源表存在，已删除，并重新建表。'
        except Exception, e:
            print '源表不存在，重新建表。'
        finally:
            cursor.execute(create_sql)
            print '建表成功。'
        cursor.close()
        conn.commit()
        conn.close()

    #插入数据
    def insert_date(self,data):
        conn = cx_Oracle.connect(self.con_str)
        cursor = conn.cursor()
        j=1  #这是一个计数器，计算从csv已经读取出来的行数
        content=[] #这是一个临时存放待插入的数据块文件变量，每次提交后将此变量清空，然后进行下一次插入
        #为了让插入数据的速度更快，使用的是插入数据块的方法，而非一行一行的插入，主要使用函数
        #cursor.executemany(sql, content) 第一个是sql，第二个是数据块，数据块使用内容为元组的列表
        for line in data:
            x = tuple(line)
            content.append(x)
            #达到提交行数，插入并提交。否则还是插入
            if j%self.commit_line==0:
                a=[':%s' %i for i in range(self.column_count+1)]
                value=','.join(a[1:])
                sql = 'insert into %s values(%s)' % (self.table_name, value)
                try:
                    cursor.executemany(sql, content)
                    conn.commit()
                    print str(j) + '行数据已插入'
                except Exception,e:
                    print '插入失败，请检查是否有某一列数据全部为空'
                #提交以后
                content = []
            j = j + 1
        #由于提交行数是一块一块的，且块数据量是插入行数，所以很可能剩下部分内容成为小块
        # 这里讲剩下的部分再插入并提交一次
        if len(content)>0:
            a = [':%s' % i for i in range(self.column_count+1)]
            value = ','.join(a[1:])
            sql = 'insert into %s values(%s)' % (self.table_name, value)
            try:
                cursor.executemany(sql, content)
                conn.commit()
                print str(j - 1) + '行数据已插入。'
            except Exception, e:
                print '插入失败，请检查是否有某一列数据全部为空'
            conn.commit()
        conn.close()
        print 'Done!'

    #将个步骤封装起来
    def start_work(self):
        title, data = io.get_csv()
        if self.frstline_for_title==1:
            print '你选择将第一行设置为标题。'
        else:
            print '第一行不是标题，将导入全部数据。'
        io.create_table(title)
        io.insert_date(data)

if __name__=='__main__':
    con_str='liwenqiang/liwenqiang@dw'
    file_path='D:\\data\\tmp_usr_uuid2.csv'
    table_name='tmp_usr_uuid'
    io=in_Oracle(con_str,file_path,table_name,separator=1,commit_line=10000,frstline_for_title=1)
    io.start_work()


