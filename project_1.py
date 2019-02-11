import requests, datetime
from dateutil import parser as parser
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import sender
import sys, traceback
import os
from functools import reduce
import csv

GMAIL_UNAME = ''
GMAIL_PASSWD = ''

cust_data = []
ind_data = []
LABELS = {}
html1 = """ """
html2 = """ """
html3 = """ """

def read_customer_data():
    global cust_data
    global cust_data
    global ind_data
    with open('customer_data.csv') as f:
        csv_data = csv.DictReader(f, delimiter=',')
        for data in csv_data:
            cust_data.append(data)
    for i in cust_data:
        ind_data.append(i)
    for j in ind_data:
        for k,v in j.items():
            if k == 'Sensor_Names':
                j[k] = v.split(',')
            if k == 'To_List_Email':
                j[k]  = v.split(',')
            if k == 'BCC_List_Email':
                j[k] = v.split(',')
            if k == 'Sensor_Type':
                j[k] = v.split(',')


def build_labes(data):
    global LABELS
    LABELS = {}
    field_names = data['Sensor_Names']
    url_info    = {'ch': [{
                    'channel': data['API_Channel-1'],
                    'key': data['API_Key-1']},
                    {'channel': data['API_Channel-2'],
                        'key': data['API_Key-2']}],
                    'base': 'https://api.thingspeak.com/channels/',
                    }
    sensor_count = len(data['Sensor_Names'])
    for m in range(1,sensor_count+1):
        LABELS[m] = field_names[m-1]
    return(LABELS, url_info, sensor_count)

def analyz_func(analyz, html1, data):
    if data['Detail_data'] == 'Y':
        if 'mpm' in data['Sensor_Type']:
            return html1.format(analyz['total'], analyz['on'], analyz['off'], analyz['onp'], analyz['meters'], analyz['speed'])
        else:
            return html1.format(analyz['total'], analyz['on'], analyz['off'], analyz['onp'])
    else:
        return html1.format(analyz['total'])

tm1 = 0
av_s = 0
fmt = '%Y-%m-%d %H:%M:%S'

bdy=""

off_time_markup = {}

def pretty_time_delta(seconds):
    sign_string = '-' if seconds < 0 else ''
    seconds = abs(int(seconds))
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    if days > 0:
        return '%s%dd %dh %dm %ds' % (sign_string, days, hours, minutes, seconds)
    elif hours > 0:
        return '%s%dh %dm %ds' % (sign_string, hours, minutes, seconds)
    elif minutes > 0:
        return '%s%dm %ds' % (sign_string, minutes, seconds)
    else:
        return '%s%ds' % (sign_string, seconds)

def parse_data(row):
    ind = -1
    try:
        for i in range(1,9):
            if row['field%d'%i] is not None:
                ind = i

    except:
        pass
    return ind

'''
sdate: datetime
shift: 1: morning, 2: afternoon
'''
def getshiftdata(sdate, shift, field, url_info):
    global cache
    if cache[field]:
        print ('cache hit field %d' %field)
        return cache[field]

    ch = 0 if field <= 8 else 1

    td = datetime.timedelta(hours = 12)
    hr = (shift == 1 and 2 or 14)
    sdate = sdate.replace(hour = hr, minute = 30, second = 0, microsecond = 0)
    url = "%s%s/feeds.json"%(url_info['base'], url_info['ch'][ch]['channel'])
    par = {'api_key': url_info['ch'][ch]['key'],
            'start': sdate.strftime(fmt),
            'end': (sdate + td).strftime(fmt),
            }
    ret = []
    print ('cache miss. getting...')
    while True:
        try:
            resp = requests.get(url, params = par, timeout = 20)
        except Exception as e:
            print (e)
        if len(resp.json()['feeds']) == 0:
            break

        ret.extend(resp.json()['feeds'])
        x = min(resp.json()['feeds'], key=lambda x: parser.parse(x['created_at']))
        par['end'] = x['created_at']
        if len(resp.json()['feeds']) < 8000:
            break

    if ch == 0:
        for r in ret:
            for i in range(1, field_cnt + 1):
                if r['field%d'%i] is not None:
                    if r['field%d'%i].isdigit():
                        ind = parse_data(r)
                        cache[ind].append(r)
    else:
        for r in ret:
            for i in range(1, field_cnt + 1):
                if r['field%d'%i] is not None:
                    if r['field%d'%i].isdigit():
                        ind = parse_data(r)
                        cache[ind + 8].append(r)
    return cache[field]

def getunique(sdate, shift, field, url_info):
    feed = getshiftdata(sdate, shift, field, url_info)
    feed = {v['created_at']:v for v in feed}.values()
    feed = [f for f in feed if f['field%d'%(field if field < 9 else field - 8)] != None]
    return feed

def off_analysis(data, field, fnm, sdate, shift, hst_flag):
    print('off time')
    #np.save('/tmp/data', data)
    global off_time_markup
    try:
        off_time_markup[field][shift] = ''
    except:
        off_time_markup[field] = {}
        off_time_markup[field][shift] = ''
    #off_time_markup[field][shift] = ''
    off_time_lst = []
    #pdb.set_trace()
    count = 0
    #label_lst = [3,6,9,12,15,30,60,180,360,540,720,900]
    label_lst = [1,2,3,4,5,10,20,60,120,180,240,300]
    off_time_lst = [[] for _ in range(len(label_lst))]
    label_str_lst = [   "1-2 Minutes",
						"2-3 Minutes",
						"3-4 Minutes",
						"4-5 Minutes",
						"5-10 Minutes",
						"10-20 Minutes",
						"20-60 Minutes",
						"1-2 Hours",
						"2-3 Hours",
						"3-4 Hours",
						"4-5 Hours",
						">5 Hours"]
    def process(count):
        #global off_time_lst
        #print('count')
        #print(count)

        for i,l in enumerate(label_lst[::-1]):

            if count >= l:
                index =  len(label_lst) - i - 1
                off_time_lst[index].append(count)
                break
        count = 0
    i = 0
    for a in data:
        if a == 0:

            i+=1
            #print(i)
            count +=1
            #print(count)
        else:
            #do processing here
            #print(count)
            process(count)
            count = 0

    process(count)

    #print 'Time duration|Frequency|Total'
    #print '---|---|---'


    for i in range(len(label_lst)):
        #print label_str_lst[i], '|',  len(off_time_lst[i]), '|', pretty_time_delta(sum(off_time_lst[i]) * 20)
        off_time_markup[field][shift] += "<tr><td id='f_st'>" + label_str_lst[i] + "</td><td>"
        off_time_markup[field][shift] += str(len(off_time_lst[i])) + '</td><td>' + pretty_time_delta(sum(off_time_lst[i]) * 60) + '</td></tr>'
    print('off_time_markup:',off_time_markup)

    if hst_flag == 'Y':
        x = np.arange(len(off_time_lst))
        y = [len(off_time_lst[i]) for i in x]
        plt.bar(x, y, color='teal',  edgecolor='black')
        plt.xticks(x, label_str_lst, rotation=-20)
        plt.gcf().set_size_inches(12,5)
        plt.grid(axis='y',linestyle='dotted')
        plt.yticks(range(max(y)+1))
        plt.suptitle('%s %s Shift %s \n Off Time Histogram'%( LABELS[field], ('Morning' if shift == 1 else 'Night'), sdate.strftime('%d %b, %y')))
        plt.savefig('%s-%s.png'%(fnm, LABELS[field]))


def chart(sdate, shift, fnm, url_info, data, field_cnt):
    global bdy, cache, tm1, av_s
    cache = {i:[] for i in range(1,11)}
    analyz = {}
    if data['Detail_data'] == 'Y':
        analyz['total']     = '<td id="f_st">Duration</td>'
        analyz['on']        = '<td id="f_st">On Time</td>'
        analyz['off']       = '<td id="f_st">Off Time</td>'
        analyz['onp']       = '<td id="f_st">On %</td>'
        if 'mpm' in data['Sensor_Type']:
            analyz['meters']    = '<td id="f_st">Total Meters</td>'
            analyz['speed']     = '<td id="f_st">AVG. Speed</td>'
    else:
        analyz['total']     = '<td id="f_st">Duration</td>'

    for field in range(1, field_cnt+1):
        try:
            lst = getunique(sdate, shift, field, url_info)

            X = np.array([parser.parse(v['created_at']) + datetime.timedelta(hours
                = 5, minutes = 30) for v in lst])
            Y = np.array([int(v['field%d'%(field if field < 9 else field - 8)]) for v in lst])

            ind = np.argsort(X[:])
            if field != field_cnt or field == 1:
                if data['Off_Time_flag'] == 'Y':
                    print('I am here')
                    off_analysis(Y[ind], field, fnm, sdate, shift, hst_flag = data['Histogram_flag'])
            #pdb.set_trace()
            plt.gcf().clear()
            ax=plt.subplot(1,1,1)
            if(len(X) > 0):
                plt.plot(X[ind], Y[ind], '#1f77b4', label = LABELS[field])
                ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
                #ax.xaxis.set_major_locator(mdates.HourLocator())
                locator = mdates.HourLocator(interval=1)
                locator.MAXTICKS = 20000
                ax.xaxis.set_minor_locator(locator)

            plt.xlabel("Time")
            plt.grid(True)
            plt.suptitle('%s %s Shift %s'%( LABELS[field], ('Morning' if shift == 1 else 'Night'), sdate.strftime('%d %b, %y')))
            plt.gcf().set_size_inches(12,5)
            plt.savefig("%s%s.png"%(fnm, LABELS[field]))
            plt.gcf().clear()

            #print ('creating html analysis')

            tm1 = 0
            av_s = 0

            an_samples = len(lst)
            an_total = len(lst) * 60

            if len(lst) == 0:
                an_on = an_off = an_onp = 0
            else:
                an_on = reduce(lambda x, y: x+y, map(lambda x: (1 if x > 0
                    else 0), Y)) * 60
                an_off = an_total - an_on
                an_onp = an_on * 100.0 / an_total

            if data['Sensor_Type'][field-1] == 'mpm':
            #if field == 1 or field == 2:
                if len(lst) != 0:
                    tm1 = reduce(lambda x,y: x+y, Y)
                    lenght_p = len(Y[ind])
                    av_s = int(tm1 / lenght_p)
                else:
                    tm1 = '0'
                    av_s = '0'
            else:
                tm1 = ' '
                av_s = ' '

            t = pretty_time_delta(an_total)
            t = t[:-3].strip()
            o = pretty_time_delta(an_on)
            o = o[:-3].strip()
            of = pretty_time_delta(an_off)
            of = of[:-3].strip()

            if bool(t) == False:
                t = '0m'
            if bool(o) == False:
                o = '0m'
            if bool(of) == False:
                of = '0m'
            if data['Detail_data'] == 'Y':
                if data['Sensor_Type'][field - 1] == 'temp':
                    analyz['total'] += "<td>%s</td>" %t
                    analyz['on']    += "<td></td>"
                    analyz['off']   += "<td></td>"
                    analyz['onp']   += "<td></td>"
                    if 'mpm' in data['Sensor_Type']:
                        analyz['meters'] += "<td></td>"
                        analyz['speed'] += "<td></td>"
                else:
                    analyz['total']     += "<td>%s</td>"%t
                    analyz['on']        += "<td>%s</td>"%o
                    analyz['off']       += "<td>%s</td>"%of
                    analyz['onp']       += "<td>%.2f</td>"%an_onp
                    if tm1 != ' ':
                        analyz['meters']    += "<td>%s</td>"%tm1
                        analyz['speed']     += "<td>%s</td>"%av_s
                    else:
                        if 'mpm' in data['Sensor_Type']:
                            analyz['meters'] += "<td></td>"
                            analyz['speed'] += "<td></td>"
            else:
                analyz['total']     += "<td>%s</td>" %t


        except KeyError as e:
            print ('key err'), str(e)
        except Exception as err:
            print ('error in creating chart'), err
            a=sys.exc_info()
            traceback.print_exc()

    s_str = '<table><caption>%s Shift </caption>'%('Morning' if shift == 1 else 'Night')

    html1  = """
                <tr>
                    <th>Metric</th>"""

    for i in range(0, field_cnt):
        i = i + 1
        html1 += "<th>%s</th>" % LABELS[i]

    if data['Detail_data'] == 'Y':
        html1 +="""</tr><tr style="border: solid;">{}</tr>"""*(len(analyz))
    else:
        html1 += """</tr><tr style="border: solid;">{}</tr>"""

    html1 +="""</table>
                    <h3>&nbsp;</h3> """
    anly_data = analyz_func(analyz, html1, data)
    bdy += s_str + anly_data
    #print off_time_markup


if __name__ == '__main__':
    read_customer_data()
    for data in ind_data:
        LABELS = {}
        html1 = """"""
        html2 = """"""
        html3 = """"""
        bdy = ""
        s_str=""
        labels_data, url_info, field_cnt = build_labes(data)
        print ('Getting data for first shift')
        chart(datetime.datetime.now()-datetime.timedelta(days = 1), 1, 'a', url_info, data, field_cnt)
        print('chart_1:',html1)
        print ('Getting data for second shift')
        chart(datetime.datetime.now()-datetime.timedelta(days = 1), 2, 'b', url_info, data, field_cnt)
        print('chart_2:', html1)
        print ('sending mail')
