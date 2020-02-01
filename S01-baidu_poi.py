import requests
import time
import pymongo
from ChangeCoordinate import ChangeCoord
import shapefile
import pandas as pd


def get_data(query, loc):
    ########################################################
    ## 代码段：循环20页，获取20页的url地址。
    urls = []
    for i in range(0, 20):
        url = 'https://api.map.baidu.com/place/v2/search?query=' + query \
              + '&bounds=' + loc + '&page_size=20&page_num=' + str(
            i) + '&output=json&ak=' + api_key
        urls.append(url)
    ########################################################
    ## 代码段：执行urls遍历，解析数据并存入数据库
    for url in urls:
        try: # 加入异常处理，防止网络状况中断出现问题。
            ###############################################
            ## 代码段：get请求获取数据，
            ## json.loads转为jsond对象。
            data = requests.get(url).json()
            ###############################################
            ## 代码段：解析json对象中的有用数据，按字典键值对存储。。
            if data['message'] == 'ok':
            # 首先判断 results 是否在data（json）对象中，
            # 如不在则无数据，不执行下面操作
                if data['total'] != 0:
                # 再次判断 total 是否为零，零的情况就是没有数据
                    for item in data['results']:
                    # 循环results，获取内部数据
                        js = {}
                        js['name'] = item['name']
                        js['lat'] = item['location']['lat']
                        js['lng'] = item['location']['lng']
                        js['address'] = item['address']
                        js['uid'] = item['uid']
                        js['province'] = item['province']
                        js['city'] = item['city']
                        js['area'] = item['area']

                        js['h1'] = h1
                        js['h2'] = h2
                        ###############################################
                        ## 代码段：使用自定义的cconv库执行坐标转换。。
                        ccg = ChangeCoord()
                        js['lat_wgs84'] = ccg.bd09_to_wgs84(js['lng'], js['lat'])[1]
                        js['lng_wgs84'] = ccg.bd09_to_wgs84(js['lng'], js['lat'])[0]

                        ###############################################
                        ## 代码段：判断是否是需要下载的城市的数据。
                        # if cityC == js['city']:
                        print(js)
                        tb.insert_one(js)
                            # pass
                        # else:
                           # print('非设定市数据，跳过...')
                else:
                    print('本页及以后无数据')
                    break
            else:
                pass
        ###############################################
        ## 代码段：异常的处理，如果出现网络中断的情况，写入URL。
        ## 这里也可回调该函数，但需要注意不要出现死循环的情况
        except:
            print('error')
            with open('./log1.txt', 'a') as fl:
                fl.write(url + '\n')

#################################################################
def lng_lat(loc_all, div):
    #################################################################
    ## 代码段：获得西南、西北经纬度坐标的值
    lng_sw = float(loc_all.split(',')[0])
    lng_ne = float(loc_all.split(',')[2])
    lat_sw = float(loc_all.split(',')[1])
    lat_ne = float(loc_all.split(',')[3])
    #################################################################
    ## 代码段：获取切割的经度值
    lng_list = [str(lng_ne)]
    while lng_ne - lng_sw >= 0:
        lng_ne = lng_ne - div
        lng_list.append('%.2f' % lng_ne)
    #################################################################
    ## 代码段：获取切割的维度值
    lat_list = [str(lat_ne)]
    while lat_ne - lat_sw >= 0:
        lat_ne = lat_ne - div
        lat_list.append('%.2f' % lat_ne)

    lng = sorted(lng_list)
    lat = lat_list

    #################################################################
    ## 代码段：进行坐标串组合
    dt = ['{},{}'.format(lat[i], lng[i2]) for i in range(0, len(lat)) for i2 in range(0, len(lng))]
    double_lst = [dt[i * len(lng):(i + 1) * len(lng)] for i in range(len(lat))]
    coords_com = [[double_lst[n + 1][i], double_lst[n][i + 1]] for n in range(0, len(lat) - 1) for i in
                  range(0, len(lng) - 1)]
    return ['{},{}'.format(loc_to_use[0], loc_to_use[1]) for loc_to_use in coords_com]  # 返回坐标串


if __name__ == "__main__":
    print("开始爬数据，请稍等...")
    start_time = time.time()
    ### 集中调参区域
    ################################################################################
    # 调参1：替换key
    api_key ='awt5YO7ifHFmzTXcv1zHhDU2txVUU1sM'
    # 调参2：替换一级、二级行业分类，具体参见http://lbsyun.baidu.com/index.php?title=lbscloud/poitags
    pois = {
            '房地产': ['住宅区'],
            '购物': ['购物中心', '百货商场']
            }
    # 调参3：mongodb的服务器路径
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    # 调参4：更改写入的数据库名称，如DEMO0713
    db = myclient["单进程CQ"]
    # 调参5：更改西南、东北坐标
    loc_all = '106.534263,29.574123, 106.58543,29.611308'
    # 调参6：替换城市名称，如上海市，请务必写入"市"
    cityC = '重庆市'
    # 调参7：更改坐标分割的区间，如0.02
    div = 0.01

    #################################################################
    ## 代码段：调用函数执行坐标分割与坐标串组合
    divds = lng_lat(loc_all, div)
    print('共有{}个网格'.format(len(divds)))

    #################################################################
    # 代码段：遍历行业分类，执行函数，获取数据以及shp文件创建
    for h1, v in pois.items():
        print('爬取：', h1)
        tb = db['POI' + h1]
        for loc_to_use in divds:
            print(loc_to_use)
            for h2 in v:
                get_data(h2, loc_to_use)

    #################################################################
    ## 代码段：读取创建好的数据，并生成shp文件。
    print('数据下载完毕，执行excel文件输出与shp文件创建。。。')
    for tp in pois.keys():
        print('{}数据下载完毕，执行shp文件创建。。。'.format(tp))
        x = db['POI' + tp].find()    #####
        x = [i for i in x]
        df = pd.DataFrame(x)
        df = df.drop_duplicates(subset='uid')
        dfc =df [['address', 'area', 'city', 'h1', 'h2', 'lat', 'lat_wgs84', 'lng',
       'lng_wgs84', 'name', 'province', 'uid']]
        dfc.to_excel(r'.\data\{}poi.xls'.format(tp))
        #################################################################
        ## 代码段：定义shp文件路径以及shp文件的字段名称、类型。
        w = shapefile.Writer(r'.\shp\{}poi'.format(tp))  # 先到该代码的文件夹里创建shp文件夹
        w.field('name', 'C')   # C代表文本类型的字段
        w.field('address', 'C')
        w.field('city', 'C')
        w.field('area', 'C')
        w.field('uid', 'C')
        w.field('h1', 'C')
        #################################################################
        ## 代码段：遍历表格中的数据。
        dv = df.reset_index()
        for i in range(len(dv)):
            w.point(dv['lng_wgs84'][i],dv['lat_wgs84'][i]) # 创建点
            w.record(                                      # 创建字段
                dv['name'][i],
                dv['address'][i],
                dv['city'][i],
                dv['area'][i],
                dv['uid'][i],
                dv['h1'][i])
    print('shp文件全部生成。。。')

    end_time = time.time()
    print("数据爬取完毕，用时%.2f秒" % (end_time - start_time))