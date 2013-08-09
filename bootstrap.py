import itertools, csv, sys, random
from numpy import subtract, dot, mean, sqrt, array, var,std,median, sort, array_split,arange, array, bincount, ndarray, ones, where
from numpy.linalg import norm
import numpy.random
from scipy.spatial import cKDTree
from collections import defaultdict
from scipy.stats.mstats import mquantiles
from scipy import stats
from scipy.spatial.distance import pdist
import pp
import cPickle as pickle
'''
"Of venue clusters ("communities") within a given geospatial size, 
 how much more homogenous is this one in race, compared to all the ones that are of 
 similar size and within areas of the city that are similarly dense?"
 '''
 
TIME_KERNEL = [.5,1.,.5]
data_dir = "~/data/"
income_vars=set([
    "Median.household.income"  ])
   # "Per capita income in the past 12 months (in 2011 inflation-adjusted dollars)"]
race_vars=set([
     "White" ,
     "African.American",
     "Asian",
     "Other.race",              
     "Two.or.more.races",
     "Hispanic"  ] )   
location_vars = set([ 
    "Longitude",                                                                                          
    "Latitude"]) 
id_vars =set([                                                                                      
    "PlaceName",                                                                                          
    "Venue",                                                                                              
#    "topLevel",                                                                                           
#    "bottomLevel",
])
time_vars= set([str(i[0])+"." + i[1] for i in itertools.product(range(0,24),
                                            ["Mon","Tues","Wed","Thurs","Fri","Sat","Sun"])])


def get_venues(file_name):
    indicies_id = []
    indicies_incomes = []
    indicies_racial_distro = []
    indicies_location = []
    indicies_time = []
    
    indicies_weekday_morn = []
    indicies_weekday_noon = []
    indicies_weekday_evening = []
    indicies_weekday_night = []
    indicies_weekend_morn = []
    indicies_weekend_noon = []
    indicies_weekend_evening = []
    indicies_weekend_night = []
    
    total_checkin_index = -1
    popn_size_index = -1
    category_index = -1
    block_index = -1
    white_index = -1
    black_index=-1
    asian_index = -1
    hispanic_index = -1
    
    input_fil = open(file_name)
    csvfile = csv.reader(input_fil, delimiter=',', quotechar='\"')
    i=0
    
    race_index = 0
    header_line = csvfile.next()
    for header in header_line:
        header= header.replace("\"","")
        if header in id_vars:
            indicies_id.append(i)
        elif header in income_vars:
            indicies_incomes.append(i)
        elif header in race_vars:
            indicies_racial_distro.append(i)
            if header == "Hispanic or Latino":
                hispanic_index = race_index
            elif header == "White alone":
                white_index = race_index
            elif header =="Black or African American alone":
                black_index = race_index
            elif header == "Asian alone":
                asian_index = race_index
            race_index +=1
        elif header in location_vars:
            indicies_location.append(i)
        elif header in time_vars:
            indicies_time.append(i)
            spl = header.split(".")
            day = spl[1]
            hour = int(spl[0])
            if hour < 6:
                if day in ["Sat","Sun"]:
                    indicies_weekend_night.append(i)
                else:
                    indicies_weekday_night.append(i)
            elif hour < 11:
                if day in ["Sat","Sun"]:
                    indicies_weekend_morn.append(i)
                else:
                    indicies_weekday_morn.append(i)
            elif hour < 16:
                if day in ["Sat","Sun"]:
                    indicies_weekend_noon.append(i)
                else:
                    indicies_weekday_noon.append(i)
            elif hour < 21:
                if day in ["Fri","Sat","Sun"]:
                    indicies_weekend_evening.append(i)
                else:
                    indicies_weekday_evening.append(i)
            else:
                if day in ["Fri","Sat"]:
                    indicies_weekend_night.append(i)
                else:
                    indicies_weekday_night.append(i)
        elif header == "TotalCheckins":
            total_checkin_index = i
        elif header == "Total":
            popn_size_index = i
        elif header =="PlaceCategory":
            category_index=i
        elif header == "Block":
            block_index = i
        #else:
        #    print "not considered: " + header + "\n"
        i+=1
        
    temporal_indicies_array =  [indicies_weekday_morn,
                                indicies_weekday_noon,
                                indicies_weekday_evening,
                                indicies_weekday_night,
                                indicies_weekend_morn,
                                indicies_weekend_noon,
                                indicies_weekend_evening,
                                indicies_weekend_night]
    len_time_points = len(indicies_time)
    #load data
    for line in csvfile:
        c_id = line[indicies_id[0]]
        venue_id_to_i[c_id] = len(venue_id)
        venue_id.append(c_id)
        venue_block.append(line[block_index])
        pop_size = float(line[popn_size_index])
        venue_popn_size.append(pop_size)
        cat = line[category_index]
        if cat == "\N" or cat == "":
            venue_category.append([""])
        else:
            venue_category.append(cat.split("::"))
        venue_incomes.append([float(line[i]) for i in indicies_incomes])
        if pop_size > 0:
            venue_racial_distro.append(array([float(line[i])/pop_size for i in indicies_racial_distro]))
        else:
            venue_racial_distro.append(array([0.]*len(indicies_racial_distro)))
            
        dist_array =array([float(line[i]) for i in indicies_location])
        venue_location.append(dist_array)

        #ASSUMES COLUMNS ARE ORDERED!!!!!
        venue_time_data =[float(line[i]) for i in indicies_time]
        new_dat = [0.]*len_time_points
        ##Special treatment of last value
        new_dat[-1] = (venue_time_data[-2]*TIME_KERNEL[0] + 
                       venue_time_data[-1]*TIME_KERNEL[1] +
                       venue_time_data[0]*TIME_KERNEL[2])
        for i in range(0,len(venue_time_data)-1):
            new_dat[i] = (venue_time_data[i-1]*TIME_KERNEL[0] + 
                           venue_time_data[i]*TIME_KERNEL[1] 
                           +venue_time_data[i+1]*TIME_KERNEL[2])
            
        #temp_array = [0.]*8
        #t_i = 0
        #for temp_ind in temporal_indicies_array:
        #    temp_array[t_i] = sum([new_dat[i-min(indicies_time)] for i in temp_ind])
        #    t_i+=1
        
        temp_array = array(new_dat)
        
        
        venue_temporal_distro.append(temp_array/temp_array.sum())
        venue_total_checkin.append(float(line[total_checkin_index]))
         
def compute_mean_pairwise_dist_array(cluster, value_array):
    from scipy.spatial.distance import pdist
    return pdist([value_array[x] for x in cluster if value_array[x].sum() != 0 ]).mean()

def compute_mean_pairwise_dist_value(ar):
    tot =0.
    comp =0.
    for i in range(len(ar)):
        for j in range(i+1,len(ar)):
            d = abs(ar[i]-ar[j])
            if(d == -1):
                print 'WTF'
                sys.exit()
                continue
            tot+=d
            comp+=1
    return float(tot)/comp

def get_cat_dist(cat1,cat2):
    len_1 = len(cat1)
    len_2 = len(cat2)
    if len_1 == 0 or len_2 ==0:
        return -1
    if cat1[0] == "" or cat2[0]=="":
        return -1
    if cat1[0] != cat2[0]:
        return 4
    if len_1 == 1 or len_2 ==1 or cat1[1] != cat2[1]:
        return 3
    if len_1 == 2 or len_2 == 2: 
        return 1
    if (cat1[2] != cat2[2]):
        return 2
    if cat1[2]==cat2[2]:
        return 1
    print cat1, cat2
    sys.exit(-1)
    return 0

def compute_interest_stat(cluster,venue_category):
    ar = [venue_category[x] for x in cluster]
    tot =0.
    comp =0.
    for i in range(len(ar)):
        for j in range(i+1,len(ar)):
            d = get_cat_dist(ar[i],ar[j])
            if(d == -1):
                print 'WTF'
                sys.exit()
                continue
            tot+=d
            comp+=1
    return float(tot)/comp


def get_bootstrap_quality(samples,cluster,n_venues):
    from numpy import std
    from collections import defaultdict
    n_times_sampled = [0] * n_venues
    for sample in samples:
        for venue in sample:
            n_times_sampled[venue]+=1
            
    sd = std(n_times_sampled)
    count_tot = defaultdict(int)
    cluster_tot = [n_times_sampled[x] for x in cluster]
    for value in n_times_sampled:
        count_tot[value]+=1

    return [sd,cluster_tot,count_tot]
            
def gen_cluster_sig(line,n_venues,
                    venue_id, venue_block,
                    venue_category, venue_incomes, 
                    venue_racial_distro,venue_popn_size,
                    venue_location,venue_temporal_distro,
                    venue_total_checkin,venue_id_to_i):
    import numpy.random
    import itertools, csv, sys, random
    from numpy import digitize, histogram, ndarray, subtract, dot, mean, sqrt, array, var,std,median, sort, array_split,arange, array, bincount, ndarray, ones, where
    from numpy.linalg import norm
    from scipy.spatial import cKDTree
    from collections import defaultdict
    from scipy.stats.mstats import mquantiles
    from scipy import stats
    from scipy.spatial.distance import pdist,cdist
    return_values = []
    venues_copy = []
    other_stat_venues_copy = set()
    
    class WalkerRandomSampling(object):
    
        def __init__(self, weights, keys=None):
            """Builds the Walker tables ``prob`` and ``inx`` for calls to `random()`.
            The weights (a list or tuple or iterable) can be in any order and they
            do not even have to sum to 1."""
            n = self.n = len(weights)
            if keys is None:
                self.keys = keys
            else:
                self.keys = array(keys)
     
            if isinstance(weights, (list, tuple)):
                weights = array(weights, dtype=float)
            elif isinstance(weights, numpy.ndarray):
                if weights.dtype != float:
                    weights = weights.astype(float)
            else:
                weights = array(list(weights), dtype=float)
     
            if weights.ndim != 1:
                raise ValueError("weights must be a vector")
     
            weights = weights * n / weights.sum()
     
            inx = -ones(n, dtype=int)
            short = where(weights < 1)[0].tolist()
            long = where(weights > 1)[0].tolist()
            while short and long:
                j = short.pop()
                k = long[-1]
     
                inx[j] = k
                weights[k] -= (1 - weights[j])
                if weights[k] < 1:
                    short.append( k )
                    long.pop()
     
            self.prob = weights
            self.inx = inx
     
        def random(self, count=None):
            """Returns a given number of random integers or keys, with probabilities
            being proportional to the weights supplied in the constructor.
     
            When `count` is ``None``, returns a single integer or key, otherwise
            returns a NumPy array with a length given in `count`.
            """
            if count is None:
                u = numpy.random.random()
                j = numpy.random.randint(self.n)
                k = j if u <= self.prob[j] else self.inx[j]
                return self.keys[k] if self.keys is not None else k
     
            u = numpy.random.random(count)
            j = numpy.random.randint(self.n, size=count)
            k = where(u <= self.prob[j], j, self.inx[j])
            return self.keys[k] if self.keys is not None else k

    kd_tree = cKDTree(venue_location)
    desired_samples = 1000
    P_VALUE = .05

    i=0
    while i < n_venues:
        venues_copy.append(i)
        other_stat_venues_copy.add(i)
        i+=1
        
    cluster = [venue_id_to_i[c_id] for c_id in line.strip().split(" ")]
    cluster_size = len(cluster)

    significance = []
    
    ####do geo bootstrapping
    cluster_popn_density = stats.gaussian_kde([venue_popn_size[x] for x in cluster])
    sample_weights = cluster_popn_density.evaluate(venue_popn_size)
    
    sampler_popn_density = WalkerRandomSampling(sample_weights)
    geo_samples = []
    while len(geo_samples) < desired_samples:
        geo_sample = sampler_popn_density.random(cluster_size)
        geo_samples.append(geo_sample)
    
    cluster_geo_stat = compute_mean_pairwise_dist_array(cluster, venue_location)
    geo_stats = []
    for sample_g in geo_samples:
        geo_stats.append(compute_mean_pairwise_dist_array(sample_g, venue_location))
    quantile_range = mquantiles(geo_stats,prob=[1-P_VALUE,P_VALUE])
    if cluster_geo_stat > quantile_range[0]:
        significance.append("Higher")
    elif cluster_geo_stat < quantile_range[1]:
        significance.append("Lower")
    else:
        significance.append("Null")
    print quantile_range
    print cluster_geo_stat
    return_values += [cluster_geo_stat, quantile_range[1],quantile_range[0]]
    [sd_geo,count_x0_in_samples_geo,count_tot_geo] = get_bootstrap_quality(geo_samples,cluster,n_venues)

    #N will include both those places in C and those not in it.
    
    cluster_locations = array([venue_location[i] for i in cluster])
    cluster_centroid =cluster_locations.mean(0)
    distances = sort(array([norm(cluster_centroid-x) for x in cluster_locations]))
    dist_spl = array_split(distances,5)
    bins = []
    for i in dist_spl:
        if len(i) > 0:
            bins.append(i[-1])
    max_distance= distances.max()
    
    cluster_checkin_density = stats.gaussian_kde([venue_total_checkin[x] for x in cluster])
    sample_weights = cluster_checkin_density.evaluate(venue_total_checkin)
    sampler_checkin = WalkerRandomSampling(sample_weights)
    samples = []
    while len(samples) < desired_samples:
        sample_st = []
        v_0 = sampler_popn_density.random()
        if v_0 not in other_stat_venues_copy:
            continue
        point_loc = venue_location[v_0]
        ret = kd_tree.query_ball_point(point_loc,max_distance)
        res = array([norm(point_loc-venue_location[x]) for x in ret])
        sample_distance_bins = digitize(res,bins)
        sample_distances = []
        for i in range(len(bins)):
            sample_distances.append([])
        for i in range(len(ret)):
            sample_distances[sample_distance_bins[i]].append(ret[i])
        
        for i in range(len(bins)):
            if len(dist_spl[i]) == 0:
                continue
            if len(sample_distances[i]) < len(dist_spl[i]):
                break
            weights = array([sample_weights[x] for x in sample_distances[i]])
            weights = weights/weights.sum()
            random_output = numpy.random.choice(
                                         sample_distances[i], 
                                         len(dist_spl[i]), 
                                         p=weights)
            random_output = numpy.random.choice(sample_distances[i],len(dist_spl[i]))
            for v in random_output:
                sample_st.append(v)
        if len(sample_st) == cluster_size:
            samples.append(sample_st)
            if len(samples) % 25 == 0:
                print len(samples)
        else:
            other_stat_venues_copy.remove(v_0)
       
#        N_0 = kd_tree.query_ball_point(venue_location[v_0],cluster_circumference+cluster_circumference*threshold)
#        if len(N_0) < cluster_size:
#            continue 
#        if abs(len(N_0) - N_points_in_cluster_circ) < (threshold*N_points_in_cluster_circ):
#            samples.append(random.sample(N_0,cluster_size))
#            sample = []
#            while len(sample) == 0:
#                sample = get_min_max_sample(random.sample(N_0,cluster_size),
#                                         venue_total_checkin,num_checkins,venues_copy,
#                                         num_checkins)
#            samples.append(sample)
            
    cluster_race_stat = compute_mean_pairwise_dist_array(cluster, venue_racial_distro)
    cluster_temporal_stat = compute_mean_pairwise_dist_array(cluster, venue_temporal_distro)
    cluster_income_stat = compute_mean_pairwise_dist_value([venue_incomes[x][0] for x in cluster])
    cluster_total_checkin_stat = compute_mean_pairwise_dist_value([venue_total_checkin[x] for x in cluster])
    cluster_interest_stat = compute_interest_stat(cluster,venue_category)

    race_stats = []
    temporal_stats = []
    income_stats = []
    total_checkin_stats = []
    interest_stats = []
    for sample_st in samples:
        race_stats.append(compute_mean_pairwise_dist_array(sample_st, venue_racial_distro))
        temporal_stats.append(compute_mean_pairwise_dist_array(sample_st, venue_temporal_distro))
        income_stats.append(compute_mean_pairwise_dist_value([venue_incomes[x][0] for x in sample_st]))
        total_checkin_stats.append(compute_mean_pairwise_dist_value([venue_total_checkin[x] for x in sample_st]))
        interest_stats.append(compute_interest_stat(sample_st,venue_category))
    
    cluster_stats = [cluster_race_stat,cluster_temporal_stat,
                    cluster_income_stat,cluster_total_checkin_stat,
                    cluster_interest_stat]
    sample_stats = [race_stats,temporal_stats,
                    income_stats,total_checkin_stats,
                    interest_stats]
    
    n_comp = len(cluster_stats)
    alpha = P_VALUE

    for i in range(n_comp):
        quantile_range = mquantiles(sample_stats[i],prob=[1-alpha,alpha])
        print cluster_stats[i]
        print quantile_range
        if cluster_stats[i] > quantile_range[0]:
            significance.append("Higher")
        elif cluster_stats[i] < quantile_range[1]:
            significance.append("Lower")
        else:
            significance.append("Null")
        return_values += [cluster_stats[i], quantile_range[1],quantile_range[0]]
        
    [sd,count_x0_in_samples,count_total] = get_bootstrap_quality(samples,cluster,n_venues)

    return [significance,sd,count_x0_in_samples,sd_geo,count_x0_in_samples_geo]+return_values

def write_quality_out(file,sd,dictionary,cluster_num):
    file.write("SD,"+str(sd)+"\n")
   # for key,value in dictionary.iteritems():
    #    file.write(str(key) +","+str(value)+","+ str(cluster_num)+"\n")
        

############PROGRAM START################

#cluster_fn = sys.argv[1]
cluster_fn = "final_clustering.txt"
cluster_file = open(cluster_fn)

#venues_fn = sys.argv[2]
venues_fn = "venues.csv"
do_pickle = False

venue_id = []
venue_block = []
venue_category = []
venue_incomes = []
venue_racial_distro = []
venue_popn_size = []
venue_location = []
venue_temporal_distro = []
venue_total_checkin = []
venue_id_to_i = dict()

if do_pickle:
    venue_id = pickle.load(open( "vid.p") )
    venue_category =pickle.load( open( "vcat.p") )
    venue_incomes=pickle.load( open( "vinc.p") )
    venue_racial_distro =pickle.load( open( "vrace.p" ) )
    venue_popn_size =pickle.load( open( "vpop.p" ) )
    venue_location =pickle.load( open( "vloc.p" ) )
    venue_temporal_distro =pickle.load( open( "vtemp.p" ) )
    venue_total_checkin =pickle.load(open( "vtot.p" ) )
    venue_id_to_i =pickle.load( open( "vidtoi.p") )
   
else:
    get_venues(venues_fn)
    pickle.dump(venue_id, open( "vid.p", "wb" ) )
    pickle.dump(venue_category, open( "vcat.p", "wb" ) )
    pickle.dump(venue_incomes, open( "vinc.p", "wb" ) )
    pickle.dump(venue_racial_distro, open( "vrace.p", "wb" ) )
    pickle.dump(venue_popn_size, open( "vpop.p", "wb" ) )
    pickle.dump(venue_location, open( "vloc.p", "wb" ) )
    pickle.dump(venue_temporal_distro, open( "vtemp.p", "wb" ) )
    pickle.dump(venue_total_checkin, open( "vtot.p", "wb" ) )
    pickle.dump(venue_id_to_i, open( "vidtoi.p", "wb" ) )

n_venues= len(venue_id)
print 'Num Venues: ' + str(n_venues)
output = []

job_server = pp.Server(ncpus=4, ppservers=())
jobs = [job_server.submit(gen_cluster_sig,
                          (line,n_venues,
                           venue_id, venue_block,
                           venue_category, venue_incomes, 
                           venue_racial_distro,venue_popn_size,
                           venue_location,venue_temporal_distro,
                           venue_total_checkin,venue_id_to_i,),
                           (compute_mean_pairwise_dist_array,
                            compute_mean_pairwise_dist_value, 
                            get_cat_dist,
                            compute_interest_stat,
                            get_bootstrap_quality,),
                            ("csv",))
                            for line in cluster_file]                    

#for line in cluster_file:
#    print (gen_cluster_sig(line,n_venues,
#                           venue_id, venue_block,
#                           venue_category, venue_incomes, 
#                           venue_racial_distro,venue_popn_size,
#                           venue_location,venue_temporal_distro,
#                           venue_total_checkin,venue_id_to_i,))
#    
output_fil = open("out_sig.csv","w")
output_fil.write("Geo,Race,Temporal,Income,TotalCheckin,Interest,")
for name in ["Geo","Race","Temporal","Income","TotalCheckin","Interest"]:
    output_fil.write(name+"_Stat,"+name+"_Lower,"+name+"_Higher,")
output_fil.write("\n")
geo_sample_test_out = open("out_geo_test.csv","w")
geo_sample_test_out.write("Name,Value,ClusterNum\n")
sample_test_out = open("out_test.csv","w")
sample_test_out.write("Name,Value,ClusterNum\n")

cl_num = 0
for job in jobs:
    o = job()
    o1 = o[0]
    for resp in o1:
        output_fil.write(resp+",")
    print o
    write_quality_out(geo_sample_test_out,o[3],o[4],cl_num)
    write_quality_out(sample_test_out,o[1],o[2],cl_num)
    for data in o[5:]:
        output_fil.write(str(data)+",")
    output_fil.write("\n")
    cl_num+=1
    
output_fil.close()
geo_sample_test_out.close()
sample_test_out.close()

        