require(stringr)
require(snowfall)
require(plyr)
require(XML)
require(RCurl)
require(reshape)
require(stringr)
require(reshape2)
require(ggplot2)

##Sample checkin data in the checkin_data.txt file:
##2011-08-07 00:27:45  31862406	4b9add62f964a52078dd35e3	Food::Mexican Restaurant	-74.032	40.6218	Trace
##2011-08-07 00:29:25	68818916	4a31831df964a520e9991fe3	Arts & Entertainment::Movie Theater::Multiplex	-74.0377	40.7269	AMC Loews Newport Centre 11

##Set this to the directory this file is in
home_dir <- ""
data_dir <- paste0(home_dir,"/data/")

source(paste0(home_dir,"initial_data_generation_functions.R"))
source(paste0(home_dir,"data_analysis_functions.r"))

#########################################DATA MANIPULATION############################
queries <- get_queries(home_dir)
##Need data to run this...
checkin_data <- get_checkin_data(home_dir,"new_york")
##NOTE: This makes a bunch of calls to the census API
venue_data <- get_nodes_data(home_dir, "new_york")
returned_census_queries <- perform_census_queries(venue_data,queries)
final_data <- get_census_data(venue_data,returned_q.ny,city,queries)



#########################################RUN THE LDAS############################
##Note these are not the same as the paper because the paper implies <= and the java code only does <
n_min_checkins_per_user <- c(2,5)
n_min_checkins_per_venue <- c(2,5,10)

jar_loc = paste0(data_dir,"lda.jar")
for(min_user in n_min_checkins_per_user){
  for(min_venue in n_min_checkins_per_venue){
    n_breaks = 2
    ###We first want to split the checkins data into 2 time periods

    fil_names <- split_checkin_data(data_dir,fin[fin$City == "New York",],checkins.ny,"new_york",n_breaks,min_user,min_venue)
     
    ntopics <- c(20,40,80)
    is_binary <- c(0,1)
    alpha <- c("0.6","20.0","50.0","100.0")
    beta <- c("0.01","1.0")
    cities <- c("new_york")
    total_grid <- expand.grid(ntopics,cities,
                              fil_names,is_binary,
                              alpha,beta,stringsAsFactors=FALSE)
    apply(sfGetCluster(),total_grid,1,function(t){
      #print(paste("java","-jar",jar_loc,t[1],data_dir,as.character(t[2]),t[3],"venues.txt",t[4],t[5],t[6]))
      system(paste("java","-jar",jar_loc,t[1],data_dir,as.character(t[2]),t[3],"venues.txt",t[4],t[5],t[6]))
    })
  }
  ##Now we have all the LDAs run, we need to transform the files into form for the NMI
  ##Cut using beta
  sfInit(parallel=TRUE,cpus=4)
  cutoff_options = c(.005,.01)
  to_run <- expand.grid(NTopics = ntopics, Binary=c("false","true"),
                        Alpha=alpha,Beta=beta,
                        Cutoff = cutoff_options,
                        City = cities,
                        MinUsers = n_min_checkins_per_user,
                        MinVenues = n_min_checkins_per_venue,
                        stringsAsFactors=FALSE)
  
  sfExport("n_breaks")
  sfExport("data_dir")
  sfLibrary(plyr)
  sfLibrary(stringr)
  output <-parApply(sfGetCluster(),to_run,1,function(fil){
    frames <- lapply(seq(n_breaks),function(period){
      f_name <- str_replace_all(paste0(data_dir,fil["City"],"/","topicWordWeights",fil["MinUsers"],"_",
                                       fil["MinVenues"],"_",period,"_",fil["Binary"],"_",as.character(fil["NTopics"]),
                                       "_",fil["Beta"],"_",fil["Alpha"],".txt")," ","")
      print(f_name)
      vens <- read.csv(f_name,sep="\t", header=FALSE)
      x <- ddply(vens, .(V1), function(t){data.frame(normalized=t$V3/sum(t$V3))})
      vens <- cbind(vens, x$normalized)[,c(1,2,4)]
      names(vens) <- c("Topic","Venue","Likelihood")
      vens <- vens[vens$Likelihood > as.numeric(fil["Cutoff"]),]
      if(nrow(vens)>0){
        vens$Period <-period
      }
      vens
    })
    data <- do.call(rbind,frames)
    if(nrow(data)==0){
      data.frame(m = "0",sd="0",nmi="0")
    } else{
      venues_to_ids <- data.frame(Venue = unique(data$Venue))
      venues_to_ids$ID <- 1:nrow(venues_to_ids)
      to_write <- ddply(data, .(Topic,Period), function(t){paste(merge(t,venues_to_ids)$ID,collapse=" ")})
      tmp1 <- tempfile()
      tmp2 <- tempfile()
      write.table(to_write[to_write$Period==1,"V1"],tmp1,row.names=FALSE,col.names=FALSE,quote=FALSE)
      write.table(to_write[to_write$Period==2,"V1"],tmp2,row.names=FALSE,col.names=FALSE,quote=FALSE)
      
      output <- system2(paste0(data_dir,"/mutual"),
                        args=list(tmp1,tmp2),stdout=TRUE)
      unlink(tmp1)
      unlink(tmp2)
      z <- ddply(data,.(Topic,Period),nrow)
      d = data.frame(m = mean(z$V1),sd=sd(z$V1),nmi=as.numeric(str_split_fixed(output,"\t",2)[2]))
      d
    }
  })
  
  new_out <- do.call(rbind,output)
  final <- cbind(to_run,new_out)
  write.csv(final,paste0(min_user,".csv"))
}



###This combines the output from the code above- we actually ran three replications, but because they were 
## on different machines the code does not show this (I just manually renamed the files)
# f <- rbind(read.csv("final_2_1.csv"),read.csv("final_5_1.csv"))
# f$Rep <- 1
# f2 <- rbind(read.csv("final_2_2.csv"),read.csv("final_5_2.csv"))
# f2$Rep <- 2
# f3 <- rbind(read.csv("final_2_3.csv"),read.csv("final_5_3.csv"))
# f3$Rep <- 3
# f <- rbind(f,f2,f3)

#########################################ANALYSIS############################
##The final output of the lda experiments are in lda_experiment.csv
f <- read.csv(paste0(home_dir,"lda_experiment.csv"))

##We remove the data where mean (m) is less than ten, and the 
#binary output, because our analysis showed these were not interesting.
f <- f[f$m > 10 & f$Binary=="true",]


ny_ldas <- orderBy(~-mean,ddply(f[f$City =="new_york",], 
                                .(City,Alpha,Beta,NTopics,MinUsers,MinVenues), 
                                summarise, mean=mean(nmi),sd=sd(nmi)))

##Generate the plot showing the results
g0 <- get_lda_result_plot(ny_ldas, "new_york")
ggsave("Dropbox/Foursquare/revision/ny_lda.pdf",g0, height=4,width=8)

##This load provides us with the variable "fin", a dataframe holding the information about
## all venues
load("Dropbox/Kenny/current_papers/TIST/all_nodes.rdata")
###Now we rerun the best three parameterizations with the full data 10 times
jar_loc = paste0(data_dir,"lda.jar")
ny_ldas$City <- as.character(ny_ldas$City)
produceLDA(ny_ldas[1,],checkins, fin[fin$City=="New York",])
produceLDA(ny_ldas[2,],checkins,fin[fin$City=="New York",])
produceLDA(ny_ldas[3,],checkins,fin[fin$City=="New York",])


##This was the output for the data used in the final run (we checked the results of the 
log-likelihood of the models by hand)
file <- "topicWordWeights5_5_1_true_20_0.01_0.6_5.txt"
checkins_file <- "checkins_5_5_1.csv"
output_file <- "venues.csv"
ny_f <- write_final_lda(file,checkins_file,output_file,fin[fin$City=="New York",])
z <- ddply(ny_f, .(Topic),function(t){paste(t$Venue,collapse=" ")})
write.table(z$V1, "final_clustering.txt", row.names=FALSE,col.names=FALSE,quote=FALSE)

###Once we obtained our final clustering, we ran bootstrap.py to generate the bootstrapped CIs


ny_f$Topic <- ordered(ny_f$Topic,levels=paste("Cluster",0:19))

ny_map = get_map(source="stamen",
                 location=as.numeric(ny_f[ny_f$Topic=="Cluster 11",c("Longitude","Latitude")][1,]),
                 zoom=11)
g <- get_map_plot(ny_f,ny_map)
ggsave("ny_map.pdf",g,height = 8,width=15)

theme_g2 <- theme(axis.title.x=element_text(size=40), 
                  axis.title.y=element_text(size=40), 
                  strip.text.x=element_text(size=40),
                  axis.text.x=element_text(size=20),
                  axis.text.y=element_text(size=20))

g2 <- ggplot(ny_sig, aes(Stat,y=factor(Topic),yend=factor(Topic),x=Lower,xend=Higher)) 
g2 <- g2 + geom_point(aes(x=Stat),color='red',size=6) + geom_segment() 
g2 <- g2 + facet_wrap(~Constraint,scales="free",nrow=1) + xlab("Statistic Value") + ylab("Cluster")
ggsave(file="sig_ny.pdf",g2+theme_g2,height = 8,width=23)

g8 <- get_single_cluster_map(ny_f,10)
ggsave("10.pdf",g4,height=8,width=8)

g4 <- get_single_cluster_map(ny_f,17)
ggsave("17_map.pdf",g4,height=8,width=8)
g6 <- ggplot(ny_f,aes(Poor,Median.household.income,color=Poor)) + geom_boxplot() + theme(legend.position='none')+coord_flip()


p <- ggplot(ny_f[ny_f$Topic=="Cluster 3",],aes(bottomLevel)) + geom_bar()
p <- p + coord_flip()+scale_y_continuous(breaks=c(1,3,5,7,9))
p <- p + ylab("Number of Venues") + xlab("")
p <- p + theme(axis.text.x=element_text(size=30),
               axis.text.y=element_text(size=30),
               axis.title.x=element_text(size=30),
               axis.title.y=element_text(size=30))

z <- unlist(lapply(nyt[,71:238],sum))
df <- data.frame(V1=names(z),V2=z)
ggplot(df,aes(V1,V2,group=1)) + geom_line() + theme(axis.text.x=element_text(angle=90)) + scale_x_discrete(breaks=df$V1[seq(0,length(df$V1),by=5)])
nyt <- ny_f[ny_f$Topic=="Cluster 3",]


ggsave(file="3.pdf",p,height=8,width=12)
g4 <- get_single_cluster_map(ny_f,3,12,"bottomLevel")
ggsave(file="3_map.pdf",g4,height=8,width=8)

