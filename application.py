from flask import Flask, render_template,request,flash
import random
import time
import redis
import pygal
import pyodbc




app = Flask(__name__)
server = 'mustafasecond.database.windows.net'
database = 'quiz3'
username = 'mu'
password = 'Qwer2468'
connection= pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = connection.cursor()

@app.route('/')
def hello_world():
    lines=cursor.execute("Select * from allmonth")
    # for x in lines:
        # print(x)
    return render_template('index.html')


@app.route('/ques1')
def ques1():
    return render_template('ques1.html')



@app.route('/ques2')
def ques2():
    return render_template('ques2.html')

@app.route('/ques3')
def ques3():
    return render_template('ques3.html')

@app.route('/query_random', methods=['GET', 'POST'])
def query_random():
    # cursor = connection.cursor()
    query_limit = request.args['query_limit']
    start_time = time.time()
    list_of_times = []
    for i in range(0, int(query_limit)):
        start_intermediate_time = time.time()
        cursor.execute("select TOP 1 * from allmonth order by rand()")
        end_intermediate_time = time.time()
        intermediate_time = end_intermediate_time - start_intermediate_time
        list_of_times.append(intermediate_time)
    end_time = time.time()
    totaltime=(end_time-start_time)
    time_taken = (end_time-start_time) / int(query_limit)
    return render_template('question1.html', time_taken=time_taken, list_of_times=list_of_times,totaltime=totaltime)


@app.route('/restricted_mag')
def restricted_mag():
    # cursor = connection.cursor()
    limit = request.args['query_limit1']
    lowmag = request.args['lowmag']
    highermag =request.args['highermag']

    print(lowmag,highermag)

    start_total_time = time.time()
    list_of_times = []

    for i in range(0, int(limit)):
        start_time_for_each = time.time()
        sql = 'select * from allmonth where mag>=? and mag<=? '
        paramlist = [lowmag, highermag]
        cursor.execute(sql, paramlist)
        end_time_for_each= time.time()
        time_for_each = end_time_for_each - start_time_for_each
        list_of_times.append(time_for_each)
    end_total_time = time.time()
    totaltime = end_total_time - start_total_time
    avarge_time_of_all = totaltime / int(limit)
    return render_template('question2.html', avarge_time_of_all=avarge_time_of_all, list_of_times=list_of_times,totaltime=totaltime)
#
#
@app.route('/restricted_time')
def restricted_time():
    # cursor = connection.cursor()
    limit = request.args['query_limit1']
    timelow = str(request.args['lowtime'])
    timehigh = str(request.args['highertime'])
    start_total_time = time.time()
    list_all_times = []
    for i in range(0, int(limit)):
        start_time_for_each = time.time()
        sql = 'select * from allmonth where CONVERT(VARCHAR, [time]) between ? and ?'
        paramlist = [timelow, timehigh]
        cursor.execute(sql, paramlist)
        end_time_for_each = time.time()
        time_for_each = end_time_for_each - start_time_for_each
        list_all_times.append(time_for_each)
    end_total_time = time.time()
    totaltime=end_total_time-start_total_time
    avarge_time_of_all = totaltime / int(limit)
    print(avarge_time_of_all)
    return render_template('question2.html', avarge_time_of_all=avarge_time_of_all, list_all_times=list_all_times,totaltime=totaltime)

#
@app.route('/restricted_CA')
def restricted_CA():
    # cursor = connection.cursor()
    limit = request.args['query_limit1']
    start_total_time= time.time()
    list_all_times = []
    ca = '%, CA'
    for i in range(0, int(limit)):
        start_time_for_each = time.time()
        sql = 'SELECT * FROM allmonth WHERE place LIKE ? '
        cursor.execute(sql, (ca,))
        end_time_for_each = time.time()
        time_for_each = end_time_for_each - start_time_for_each
        list_all_times.append(time_for_each)
    end_total_time = time.time()
    totaltime = end_total_time - start_total_time
    avarge_time_of_all = totaltime / int(limit)
    print(avarge_time_of_all)
    return render_template('question2.html',  avarge_time_of_all= avarge_time_of_all, list_of_times=list_all_times,totaltime=totaltime)
#
#
@app.route('/restricted_loc')
def restricted_loc():
    # cursor = connection.cursor()
    limit = request.args['query_limit1']
    lat = float(request.args['lat'])
    long = float(request.args['long'])
    start_total_time = time.time()
    list_all_times = []
    lat1 = lat + 100
    long1 = long + 100
    lat2 = lat - 100
    long2 = long - 100
    # paramlist = [lat1, long1, lat1, long2]
    for i in range(0, int(limit)):
        start_time_for_each = time.time()
        sql = 'SELECT * FROM allmonth WHERE latitude>=? and longitude>=? and latitude<=? and longitude<=? '
        cursor.execute(sql, (lat1, long1, lat2, long2))
        end_time_for_each = time.time()
        time_for_each = end_time_for_each - start_time_for_each
        list_all_times.append(time_for_each)
    end_total_time = time.time()
    totaltime = end_total_time - start_total_time
    avarge_time_of_all = totaltime / int(limit)
    return render_template('question2.html', avarge_time_of_all=avarge_time_of_all, list_all_times=list_all_times ,totaltime=totaltime)


@app.route('/redis_cache')
def redis_cache():
    # cursor = connection.cursor()
    magnitude = request.args['magnitude']
    start_time1 = time.time()
    host_name = 'mustafaalhilo.redis.cache.windows.net'
    password = 'zZ0dvj0VkQrDAW8Pu0KJfzmGD7svHHYtpPXx4HADPqQ='
    cache = redis.StrictRedis(host=host_name, port=6380, password=password, ssl=True)

    if not cache.get(magnitude):
        sql = 'select * from allmonth where mag>=? '
        cursor.execute(sql, (magnitude,))
        rows = cursor.fetchall()
        cache.set(magnitude, str(rows))
        flash('In DB Query with Magnitude: ' + str(magnitude))

    else:
        rows_string = cache.get(magnitude)
        flash('In Cache with Magnitude: ' + str(magnitude))
    end_time1 = time.time()
    time_taken = (end_time1 - start_time1)
    return render_template('redis_cache.html', time_taken=time_taken)


@app.route('/redis_time')
def redis_time():
    # cursor = connection.cursor()
    query_limit = request.args['query_limit1']
    lowtime = str(request.args['lowtime'])
    highertime = str(request.args['highertime'])
    start_time1 = time.time()
    list_of_times = []
    host_name = 'mustafaalhilo.redis.cache.windows.net'
    password = 'zZ0dvj0VkQrDAW8Pu0KJfzmGD7svHHYtpPXx4HADPqQ='
    cache = redis.StrictRedis(host=host_name, port=6380, password=password, ssl=True)
    for i in range(0, int(query_limit)):
        start_intermediate_time = time.time()
        sql = 'select * from allmonth where CONVERT(VARCHAR, [time]) between ? and ?'
        paramlist = [lowtime, highertime]
        cursor.execute(sql, paramlist)

        if not cache.get(lowtime):
            sql = 'select * from allmonth where CONVERT(VARCHAR, [time]) between ? and ?'
            paramlist = [lowtime, highertime]
            cursor.execute(sql, paramlist)
            rows = cursor.fetchall()
            cache.set(lowtime, str(rows))
            flash('In DB Query with Depth Value 1: ' + str(lowtime))
        else:
            rows_string = cache.get(lowtime)
            flash('In Cache with Depth Value 1: ' + str(lowtime))

        if not cache.get(highertime):
            sql = 'select * from allmonth where CONVERT(VARCHAR, [time]) between ? and ?'
            paramlist = [lowtime, highertime]
            cursor.execute(sql, paramlist)
            rows = cursor.fetchall()
            cache.set(highertime, str(rows))
            flash('In DB Query with Depth Value 2: ' + str(highertime))
        else:
            rows_string = cache.get(highertime)
            flash('In Cache with Depth Value 2: ' + str(highertime))

        # countList.append(rows)
        end_intermediate_time = time.time()
        intermediate_time = end_intermediate_time - start_intermediate_time
        list_of_times.append(intermediate_time)
    end_time1 = time.time()
    time_taken = (end_time1 - start_time1) / int(query_limit)

    return render_template('redis_cache.html', time_taken=time_taken, list_of_times=list_of_times)
#

@app.route('/redis_loc')
def redis_loc():
    # cursor = connection.cursor()
    query_limit = request.args['query_limit1']
    lat = float(request.args['lat'])
    long = float(request.args['long'])
    start_time1 = time.time()
    list_of_times = []
    lat1 = lat + 100
    long1 = long + 100
    lat2 = lat - 100
    long2 = long - 100
    host_name = 'mustafaalhilo.redis.cache.windows.net'
    password = 'zZ0dvj0VkQrDAW8Pu0KJfzmGD7svHHYtpPXx4HADPqQ='
    cache = redis.StrictRedis(host=host_name, port=6380, password=password, ssl=True)
    for i in range(0, int(query_limit)):
        start_intermediate_time = time.time()

        if not cache.get(lat):
            sql = 'SELECT * FROM allmonth WHERE latitude>=? and longitude>=? and latitude<=? and longitude<=? '
            cursor.execute(sql, (lat1, long1, lat2, long2))
            rows = cursor.fetchall()
            cache.set(lat, str(rows))
            flash('In DB Query with Depth Value 1: ' + str(lat))
        else:
            rows_string = cache.get(lat)
            flash('In Cache with Depth Value 1: ' + str(lat))

        if not cache.get(long):
            sql = 'SELECT * FROM allmonth WHERE latitude>=? and longitude>=? and latitude<=? and longitude<=? '
            cursor.execute(sql, (lat1, long1, lat2, long2))
            rows = cursor.fetchall()
            cache.set(long, str(rows))
            flash('In DB Query with Depth Value 2: ' + str(long))
        else:
            rows_string = cache.get(long)
            flash('In Cache with Depth Value 2: ' + str(long))

        # countList.append(rows)
        end_intermediate_time = time.time()
        intermediate_time = end_intermediate_time - start_intermediate_time
        list_of_times.append(intermediate_time)
    end_time1 = time.time()
    time_taken = (end_time1 - start_time1) / int(query_limit)

    return render_template('redis_cache.html', time_taken=time_taken, list_of_times=list_of_times)
#
#
@app.route('/redis_CA')
def redis_CA():
    # cursor = connection.cursor()
    query_limit = request.args['query_limit1']
    start_time1 = time.time()
    list_of_times = []
    ca = '%, CA'
    CA1 = "CA"
    host_name = 'mustafaalhilo.redis.cache.windows.net'
    password = 'zZ0dvj0VkQrDAW8Pu0KJfzmGD7svHHYtpPXx4HADPqQ='
    cache = redis.StrictRedis(host=host_name, port=6380, password=password, ssl=True)
    for i in range(0, int(query_limit)):
        start_intermediate_time = time.time()
        if not cache.get(CA1):
            sql = 'SELECT * FROM allmonth WHERE place LIKE ? '
            cursor.execute(sql, (ca,))
            rows = cursor.fetchall()
            cache.set(CA1, str(rows))
            flash('In DB Query with Magnitude: ' + str(CA1))
        else:
            rows_string = cache.get(CA1)
            flash('In Cache with Magnitude: ' + str(CA1))
        end_intermediate_time = time.time()
        intermediate_time = end_intermediate_time - start_intermediate_time
        list_of_times.append(intermediate_time)
    end_time1 = time.time()
    time_taken = (end_time1 - start_time1) / int(query_limit)
    return render_template('redis_cache.html', time_taken=time_taken, list_of_times=list_of_times)


if __name__ == '__main__':
    # app.run(host='0.0.0.0', port=8080)
    app.secret_key = 'zZ0dvj0VkQrDAW8Pu0KJfzmGD7svHHYtpPXx4HADPqQ'
    app.run()