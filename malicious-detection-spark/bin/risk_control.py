# coding:utf-8
# local datapath:/Users/slade/Documents/Yoho/hdfs_data/riskdata11061617.txt
import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest


# 每5分钟计算一次异常用户
def break_user(data_all):
    data_all = data_all
    single_min = data_all['minutes'][data_all['minutes'].duplicated() == False].sort_values()
    single_min.index = range(len(single_min))
    train_data_res = {}
    start_min = []
    end_min = []
    val = min(data_all['minutes'])
    i = 0
    while val <= max(single_min):
        # 每隔5分钟汇总一次数据
        train_data_res[i] = data_all[data_all['minutes'] >= val][data_all['minutes'] < (val + 5)]
        # 汇总开始时间存储
        start_min.append(val)
        # 汇总结束时间存储
        end_min.append(val + 5)
        val = val + 5
        i = i + 1
    # 一小时内数据按5分钟进行汇总
    for i in range(len(train_data_res)):
        grouped = train_data_res[i].groupby(train_data_res[i]['ip'])
        train_data_res[i] = grouped.sum()
        index_data = pd.DataFrame(train_data_res[i].index)
        train_data_res[i].index = range(train_data_res[i].shape[0])
        train_data_res[i] = pd.concat([index_data, train_data_res[i]], axis=1)
    train_data_res_backup =train_data_res.copy()
    return train_data_res, start_min, end_min,train_data_res_backup


def train(train_data_res, max_limit=0.005, nestimators=100, contamination=0.01, max_output=30):
    outlier_dict = dict()  # 定义空异常用户集合
    heavy_point = dict()  # 定义重心集合
    for i in range(len(train_data_res)):
        train_data_res[i] = train_data_res[i][['ip','qps','impcount','notexsitudidcount','imppercent','uidcount_udidcount','allcountpercent']]
    dfd = train_data_res
    # 统计共有多少个待识别的时段
    amount = len(train_data_res)
    max_col = len(train_data_res[0].columns)
    for a in range(amount):
        # 切比雪夫不等式切分数据集合
        # 去除类似ip的非计算指标
        DF_G = dfd[a].iloc[:, 1:max_col]
        # 保存原始变量
        DF_G_B = DF_G
        DF_G_B_T = np.array(DF_G_B).T
        DF_G_B = np.array(DF_G_B)
        DF_G_B_T_new = DF_G_B_T
        DF_G_B_new = DF_G_B
        # 计算协方差矩阵及逆矩阵
        S = np.cov(DF_G_B_T_new)
        S_f = pd.DataFrame(S)
        # 如果协方差矩阵某列全为0，则删除该列的feature，重新计算协方差
        if len(S_f[(S_f == 0).all()].index) > 0:
            while len(S_f[(S_f == 0).all()].index) > 0:
                badindex = S_f[(S_f == 0).all()].index
                indexnew = [i for i in range(S_f.shape[0]) if i not in badindex]
                # 如果独立变量小于二则无法检验，跳出异常检验，返回无异常用户
                if len(indexnew) <= 1:
                    print('error:the input data is not satisfay the limitation!')
                S_f = pd.DataFrame(np.cov(pd.DataFrame(DF_G_B_new[:, indexnew].T))).copy()
            # 如果协方差某两列一致，则删除任意一列
            badindex1 = []
            for i in range(S_f.shape[0]):
                for j in range(S_f.shape[0]):
                    if j > i:
                        if sum(S_f[i] - S_f[j]) == 0:
                            badindex1.append(j)
                            # 剔除相关向量
            if len(badindex1) > 0:
                indexnew1 = [i for i in range(S_f.shape[0]) if i not in badindex1]
                S_f = pd.DataFrame(np.cov(pd.DataFrame(DF_G_B_new[:, indexnew][:, indexnew1].T))).copy()
                DF_G_B_new = DF_G_B_new[:, indexnew][:, indexnew1]
                DF_G_B_T_new = np.array(DF_G_B_new).T
                t_indexnew = indexnew
                t_indexnew.insert(0,-1)
                t_indexnew = list(np.array(t_indexnew)+1)
                t_indexnew1 = indexnew1
                t_indexnew1.insert(0, -1)
                t_indexnew1 = list(np.array(t_indexnew1) + 1)
                train_data_res[a] = train_data_res[a].iloc[:, t_indexnew].iloc[:, t_indexnew1]
            else:
                DF_G_B_new = DF_G_B_new[:, indexnew]
                DF_G_B_T_new = np.array(DF_G_B_new).T
                t_indexnew = indexnew
                t_indexnew.insert(0,-1)
                t_indexnew = list(np.array(t_indexnew)+1)
                train_data_res[a] = train_data_res[a].iloc[:, t_indexnew]
            S = S_f
            # 计算逆矩阵
        SI = np.linalg.inv(S)
        # 求全量数据的重心的距离
        DF_G_B_T_heavypoint = DF_G_B_T_new.mean(axis=1)
        d1 = []
        n = DF_G_B_new.shape[0]
        # 计算切比雪夫不等式中的马氏距离
        for i in range(n):
            delta = DF_G_B_new[i] - DF_G_B_T_heavypoint
            d = np.sqrt(np.dot(np.dot(delta, SI), delta.T))
            d1.append(d)
        # 异常用户集合，初筛千分之5
        d2 = pd.Series(d1)
        N = DF_G_B_new.shape[1]
        pr = max_limit
        limit = np.sqrt(N / pr)
        outlier = d2[d2 > limit]
        # 防止抛空的异常用户,至少满足20个人的异常用户，并放入outlier_data异常集合中
        while len(outlier) < max(int(0.01 * n), 20):
            pr = pr + 0.005
            limit = np.sqrt(N / pr)
            outlier = d2[d2 > limit]
        index = outlier.index
        outlier_data = DF_G_B_new[index]
        outlier_data = np.array([I for I in outlier_data if ((I - DF_G_B_T_heavypoint) > 0).any()])
        # 防止抛空的未知用户，计算未知用户和正常用户,至少满足50个的未知用户，并放入outlier_data1未知集合中
        N1 = N
        pr1 = pr + 0.005
        limit1 = np.sqrt(N1 / pr1)
        outlier1 = d2[d2 > limit1]
        while len(outlier1) < max(int(0.15 * n), 50):
            pr1 = pr1 + 0.005
            limit1 = np.sqrt(N1 / pr1)
            outlier1 = d2[d2 > limit1]
        index1 = outlier1.index
        index1 = [I for I in index1 if I not in index]
        # outlier_data1为未知用户
        outlier_data1 = DF_G_B_new[index1]
        outlier_data1 = np.array([I for I in outlier_data1 if ((I - DF_G_B_T_heavypoint) > 0).any()])
        if len(outlier_data1) == 0:
            print('没有未知用户')
        # 全量用户去除异常用户和未知用户，即为正常用户
        index2 = [i for i in range(n) if i not in index and i not in index1]
        common_data = DF_G_B_new[index2]
        heavy_point[a] = common_data.mean(axis=0)
        # 如果没有数据，则返回没有未知用户，以异常用户代替未知用户+异常用户
        if len(outlier_data1) == 0:
            train = common_data
        else:
            train = np.r_[common_data, outlier_data]  # 训练数据合并
        #预估的异常用户占比：contamination。训练的树的个数：nestimators
        nestimators = nestimators
        contamination = contamination
        clf = IsolationForest(n_estimators=nestimators, contamination=contamination)
        clf.fit(train)
        # 如果没有数据，则返回没有未知用户，以异常用户代替未知用户+异常用户
        if len(outlier_data1) == 0:
            pre_format_train_data = outlier_data
        else:
            pre_format_train_data = np.r_[outlier_data, outlier_data1]
        #计算得分并排序
        Score = clf.decision_function(pre_format_train_data)
        K = np.c_[Score, pre_format_train_data]
        k_rank = np.array(sorted(K, key=lambda x: x[0]))
        # 识别排序靠前contamination参数初步定为1%
        assume_rate = np.ceil(pre_format_train_data.shape[0] * contamination * 3)
        # 设置上限，最多产出max_output个风控用户
        if assume_rate >= max_output:
            assume_rate = max_output
        outlier_data2 = k_rank[:int(assume_rate)]
        outlier_dict[a] = outlier_data2
        print("the no. %s time is ready" % a)
        print('the amount of this time is %s' % len(outlier_dict[a]))
    return outlier_dict, heavy_point, train_data_res


# 寻找ip
def ip_search(outlier_dict, train_data_res):
    ip = {}
    for i in range(len(outlier_dict)):
        outlier_dict_ip = {}
        for j in range(len(outlier_dict[i])):
            for k in range(train_data_res[i].shape[0]):
                if sum(train_data_res[i].iloc[k, 1:train_data_res[i].shape[1]] - outlier_dict[i][j][1:]) == 0:
                    ip_key0 = train_data_res[i].iloc[k, :]['ip']
                    values = pd.DataFrame(outlier_dict[i][j]).T
                    values.columns = train_data_res[i].columns[1:train_data_res[i].shape[1]].insert(0,'prob')
                    outlier_dict_ip[ip_key0] = values
            print('已经完成了第%s组内的第%s个ip识别' % (i, j))
        ip[i] = outlier_dict_ip
        print('已经完成了%s组全部内容' % i)
    return ip

# 寻找遗漏的边际异常
def ip_exception(train_data_res_backup):
    ip_exception_res = {}
    for i in range(len(train_data_res_backup)):
        exception1 = list(train_data_res_backup[i][train_data_res_backup[i]['allcountpercent']>100]['ip'])
        exception2 = list(train_data_res_backup[i][train_data_res_backup[i]['uidcount_udidcount']>10]['ip'])
        exceptionall = exception1 + exception2
        ip_exception_res[i] = exceptionall
    return ip_exception_res

# 匹配输出数据
def match_data(ip,ip_exception_res,train_data_res_backup):    
    ip_res = {}
    for i in range(len(ip)):
        all_ip = list(ip[i].keys())+ip_exception_res[i]
        all_ip = list(set(all_ip))
        res_info = pd.DataFrame()
        for j in all_ip:
            info = train_data_res_backup[i][train_data_res_backup[i]['ip']==j]
            #约束规则
            if ((info['qps'] > 5)|((((info['impcount']*1.0)/info['difmethodcount'])>0.7)&(info['difmethodcount']>5))|(info['uidcount_udidcount']>2)|(info['allcountpercent']>25)).any():
                res_info = pd.concat([res_info,info],axis=0)
        ip_res[i] =  res_info
    return ip_res

# 保存数据
def datasave(ip_res, start_min, end_min , path ):
    # 异常数据指标
    for i in range(len(ip_res)):
        if len(ip_res) == 0:
            print('无异常用户')
        output = pd.DataFrame([])
        normaldata = pd.DataFrame([])
        if len(ip_res[i])>0:        
            #需要展示的列
            info = ip_res[i][['ip','qps','udidcount','uidcount','difmethodcount','impcount','notexsitudidcount','uidcount_udidcount','allcountpercent']]
            for j in range(info.shape[0]):    
                start = pd.DataFrame([start_min[i]], columns=['start_time'])
                end = pd.DataFrame([end_min[i]], columns=['end_time'])
                df = pd.DataFrame(info.iloc[j,:]).T
                df.index = start.index                
                ip_info = pd.concat([start, end, df], axis=1)
                output = pd.concat([output, ip_info], axis=0)
        # 正常数据指标
        #     info1 = pd.DataFrame(heavy_point[i]).T
        #     info1.columns = info.columns[1:]
        #     start = pd.DataFrame([start_min[i]], columns=['start_time'])
        #     end = pd.DataFrame([end_min[i]], columns=['end_time'])
        #     label = pd.DataFrame(['Normal'])
        #     label.columns = ['prob']
        #     ipnew = pd.DataFrame(['Normal'])
        #     ipnew.columns = ['ip']
        #     pre_data = pd.concat([start, end, ipnew, label, info1], axis=1)
        #     normaldata = pd.concat([normaldata, pre_data], axis=0)
        #合并产出
            result = pd.concat([output, normaldata], axis=0)
            result = result.sort_values(['start_time'], ascending=[True])
            print(result)
            names =  path + 'riskdataIp_res' + str(i) + '.json'
            # result.to_csv(names,header=True)
            result.to_json(names,orient='records')
            # result.to_excel(names, sheet_name='Sheet1')
            # f.write(result)
            # f.write("\n")
        # f.close()
            print('%s 路径保存完成'%names)
