require(XML)
require(RCurl)
require(stringr)
require(plyr)
require(reshape)
require(data.table)


get_timing_info <- function(checkins){
  library(data.table)
  dt <- data.table(checkins)
  f <- dt[, length(time), by=c("Venue","HourDay")]
  z <- reshape(f,direction='wide',idvar='Venue', timevar='HourDay',)
  z[is.na(z)] <- 0
  
  f <- dt[, length(time), by=c("Venue")]
  setnames(f,"V1","TotalCheckins")
  dr <- cbind(z,f$TotalCheckins)
  for(name in names(dr)){setnames(dr,name,str_replace(name,"V1.",""))}
  setnames(dr,"V2","TotalCheckins")
  dr
} 

remove_bad <- function(checkins, fin, min_user,min_venue,remove_tourist,city){
  
  venues_to_not_use <- fin[fin$Total == 0 
                           |  (fin$White==0 &fin$African.American==0 & fin$Asian==0 & fin$Other.race==0  & fin$Two.or.more.races==0) 
                           | fin$PlaceCategory=="" | fin$PlaceCategory=="\\N","Venue"]
  checkins <- checkins[!(checkins$Venue %in% venues_to_not_use),]
  
  checkins_table <- data.table(checkins)
  if(remove_tourist){
    z <- checkins_table[,difftime(max(time),min(time),units="hours"),by=User]
    z <- z[as.double(z$V1) < 48,]
    checkins_table <- checkins_table[checkins_table[!(User %in% z$User),which=TRUE],]
  }
  while(T){
    init_len <- nrow(checkins_table)
    ##remove users
    z <- checkins_table[,length(time),by=User]
    z <- z[z$V1 >= min_user,]$User
    checkins_table <- checkins_table[checkins_table[(User %in% z),which=TRUE],]
    
    z <- checkins_table[,length(time),by=Venue]
    z <- z[z$V1 >= min_venue,]$Venue
    checkins_table <- checkins_table[checkins_table[(Venue %in% z),which=TRUE],]
    if(init_len - nrow(checkins_table) == 0){
      break;
    }
  }
  data.frame(checkins_table)
}

split_checkin_data <- function(write_dir, venues_data, checkins,city, nsplits=3, min_user,min_venue,remove_tourist=FALSE){
 
  checkins = remove_bad(checkins,venues_data,min_user,min_venue,remove_tourist,city)

  chunked <- split(checkins,cut(checkins$time,quantile(checkins$time,seq(0,1,by=1/nsplits))))

  fnames <- c()
  for(i in seq(length(chunked))){
    f_name <-paste0("checkins_",min_user,"_",min_venue,"_",i,".csv")
    write.csv(checkins[1:(nrow(checkins)/2),],paste0(write_dir,city,"/",f_name),row.names=FALSE)
    fnames <- c(fnames,f_name)
  }
  fnames
}
produceLDA <- function(t,checkins,fin,remove_tourist=FALSE){
  
  if(t["City"] == "new_york"){
    print("new york")
    fin <- fin[fin$City == "New York",]
  } else{
    print("SF")
    fin <- fin[fin$City == "San Francisco",]
  }

  fil_names <- split_checkin_data(data_dir,fin,checkins,as.character(t["City"]),
                                  1,as.numeric(t["MinUsers"]),as.numeric(t["MinVenues"]))
  
  new_times <- data.frame(get_timing_info(checkins))
  names(new_times) <- str_replace_all(names(new_times),"X","")
  r <- expand.grid(c(0:23),c("Mon","Tues","Wed","Thurs","Fri","Sat","Sun"))
  r <- paste(r$Var1,r$Var2,sep=".")
  new_order <- as.vector(sapply(r, function(in_r){which(names(new_times) == in_r)}))
  fin2 <- new_times[,c("Venue",names(new_times)[new_order],"TotalCheckins")]
  fin <- fin[,-(68:236)]
  fin <- merge(fin,fin2,by="Venue")
  venues_to_not_use <- fin[fin$Total == 0 
                           |  (fin$White==0 &fin$African.American==0 & fin$Asian==0 & fin$Other.race==0  & fin$Two.or.more.races==0) 
                           | fin$PlaceCategory=="" | fin$PlaceCategory=="\\N","Venue"]
  fin <-fin[!(fin$Venue %in% venues_to_not_use),]
  write.csv(fin, paste0(data_dir,t["City"],"/venues.csv"))
  
  system(paste("java","-jar",jar_loc,as.numeric(t$NTopics),data_dir,as.character(t$City),
               fil_names[1],"venues.txt",1,as.numeric(t["Alpha"]),as.numeric(t["Beta"])))
}

write_final_lda <- function(file,checkins_file,output_file,fin){
  
  vens <- read.csv(paste0(data_dir,file),sep="\t", header=FALSE)
  x <- ddply(vens, .(V1), function(t){data.frame(normalized=t$V3/sum(t$V3))})
  vens <- cbind(vens, x$normalized)[,c(1,2,4)]
  names(vens) <- c("Topic","Venue","Likelihood")
  vens <- vens[vens$Likelihood > .005,]
  
  checkins <- read.csv(paste0(data_dir,checkins_file))
  
  ##Write Venues file
  
  ##Get timing information
  new_times <- data.frame(get_timing_info(checkins))
  names(new_times) <- str_replace_all(names(new_times),"X","")
  r <- expand.grid(c(0:23),c("Mon","Tues","Wed","Thurs","Fri","Sat","Sun"))
  r <- paste(r$Var1,r$Var2,sep=".")
  new_order <- as.vector(sapply(r, function(in_r){which(names(new_times) == in_r)}))
  new_order <- as.vector(unlist(new_order))
  new_times <- new_times[,c("Venue",names(new_times)[new_order],"TotalCheckins")]
  
  ##Remove unwanted venues
  venues_to_not_use <- fin[fin$Total == 0 
                           |  (fin$White==0 &fin$African.American==0 & fin$Asian==0 & fin$Other.race==0  & fin$Two.or.more.races==0) 
                           | fin$PlaceCategory=="" | fin$PlaceCategory=="\\N","Venue"]
  fin <-fin[!(fin$Venue %in% venues_to_not_use),]

  ##Merge in data from new timings with old data
  final.venue.data <- fin[,-(68:236)]
  final.venue.data <- merge(final.venue.data,new_times,by="Venue",all.x=F,all.y=T)
  write.csv(final.venue.data, paste0(data_dir,output_file))
            
##Here, provide final topic data  
  final.topic.data <- merge(vens, final.venue.data, by="Venue")
  final.topic.data
}

get_lda_result_plot <- function(ny_ldas, city){
  dodge = position_dodge(width=.3)
  p <- ggplot(ny_ldas[ny_ldas$NTopics==20 & ny_ldas$Beta==.01,],
              aes(factor(MinUsers),
                  color=factor(MinVenues),mean,ymin=mean-sd,ymax=mean+sd)) 
  p <- p + geom_pointrange(position=dodge) + facet_wrap(~Alpha,nrow=1)
  p <- p + ylab("Mean NMI") + xlab("Minimum # Checkins Per User") 
  p <- p + scale_colour_discrete(guide=guide_legend("Min. # Checkins Per Venue"))
  p + theme(legend.position='bottom')
}

get_map_plot <- function(dat, map){
  z <- ddply(dat,.(Topic), summarise,mean_lat= mean(Latitude), mean_lon=mean(Longitude))
  g <- ggmap(map,extent='panel') + geom_point(data=dat, aes(Longitude,Latitude),size=3)
  g <- g + geom_density2d(data=dat, aes(x=Longitude,y=Latitude))
  g <- g + geom_point(data=z,aes(x=mean_lon,y=mean_lat),color='red',size=4,alpha=.6)
  g <- g + facet_wrap(~Topic,nrow=4)
  g + theme(axis.text.x=element_blank(),axis.text.y=element_blank(),
            axis.title.y=element_blank(),axis.title.x=element_blank(),
            strip.text=element_text(size=12))
}

get_sig_data <- function(file){
  sig <- read.csv(paste0("Dropbox/Foursquare/src/",file))
  sig <- sig[,-c(1:4)]
  sig$Topic <- 0:19
  sig$X <- NULL
  sig <- melt(sig,id.var="Topic")
  spl <- str_split_fixed(sig$variable,"_",2)
  sig <- cbind(sig,data.frame(Constraint = spl[,1],Plotter=spl[,2]))
  sig$variable <- NULL
  sig <- dcast(sig,Topic+Constraint~Plotter)
  sig[sig$Constraint != "Temporal" & sig$Constraint != "TotalCheckin",]
  
}

get_single_cluster_map <- function(data,cluster_num,zoom=11,color_var="PlaceName"){
  cluster <- paste("Cluster",cluster_num)
  g4 <- ggmap(get_map(source="google",location=
                        as.numeric(data[data$Topic==cluster,
                                        c("Longitude","Latitude")][4,]),zoom=zoom))
  g4 <- g4 +geom_point(data=data[data$Topic==cluster,],
                       aes_string(x="Longitude",y="Latitude",color=color_var),size=6) 
  g4 <- g4 + theme(axis.text.x=element_blank(),axis.text.y=element_blank(),
                   axis.title.x=element_blank(),axis.title.y=element_blank(),
                   legend.title=element_blank())
  g4
}