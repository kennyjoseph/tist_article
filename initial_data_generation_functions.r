require(XML)
require(RCurl)
require(stringr)
require(plyr)
require(reshape)
require(data.table)

###PUT CENSUS API KEY HERE
CENSUS_API_KEY = "252d93aeb8848132236a9e81cc36422e3314e86b"

get_queries <- function(home_dir){
  ###Pull in the list of demographics we want to consider
  demographics_to_pull <- read.csv(paste(home_dir,"/demographics_to_pull.txt",sep=""),header=FALSE)
  
  ##Get the actual data we want to query
  dem_fil <- xmlTreeParse("http://www.census.gov/developers/data/acs_5yr_2011_var.xml")
  f <- xmlRoot(dem_fil)
  z <- xmlChildren(f)[sapply(xmlChildren(f),function(t){xmlAttrs(t)["name"]%in% demographics_to_pull$V1})]
  
  query_terms <- unlist(sapply(z, function(t){sapply(xmlChildren(t),function(r){xmlAttrs(r)["name"]})}))
  query_term_names <- unlist(sapply(z, function(t){
    sapply(xmlChildren(t),
           function(r){gsub("\n"," ",gsub("!"," ",xmlValue(r)) )}
    )
  }))
  
  queries <- data.frame(terms=query_terms, names=query_term_names)
  queries[grep("E",queries$terms),]
  
}

get_nodes_data <- function(home_dir, city,already_handled_data=c()){
  checkins <- read.csv(paste(home_dir,"data/",city,"/checkin_data.txt",sep=""),sep="\t",header=FALSE)
  nodes <- checkins[!duplicated(checkins$V3),][,3:7]
  nodes$id <- nodes$V3
  nodes$names <- nodes$V7
  nodes$V3 <- NULL
  nodes$V7 <- NULL
  names(nodes) <- c("PlaceCategory","Latitude","Longitude","Venue","PlaceName")
  print(nodes[1,])
  df <- str_split_fixed(nodes$PlaceCategory, "::",2)
  df <- data.frame(topLevel=df[,1], bottomLevel=df[,2])
  nodes<- cbind(nodes, df)
  nodes[nodes$topLevel == "Colleges & Universities", "topLevel"] <- "College & University"
  nodes[nodes$topLevel == "Shops", "topLevel"] <- "Shop & Service"
  nodes[nodes$topLevel == "Home, Work, Others", "topLevel"] <- "Homes, Work, Others"
  levels(nodes$topLevel)[levels(nodes$topLevel) == "Professional & Other Places"] <- "Professional & Other"
  nodes[nodes$topLevel == "Nightlife Spot","topLevel"] <- "Nightlife Spots"
  nodes$topLevel <- factor(nodes$topLevel)
  
  ###Pull down the census blocks
  str = "http://data.fcc.gov/api/block/2010/find?latitude=LAT&longitude=LONG"
  uris_to_get <- apply(nodes[,c("Longitude","Latitude")],1,function(t){
    s <- sub("LAT",t[2],str)
    s <- sub("LONG",t[1],s)
    s})
  
  uris_to_get <- uris_to_get[!(uris_to_get %in% names(already_handled_data))]
  maxi <- 100
  r <- split(uris_to_get,ceiling(seq_along(uris_to_get)/maxi))
  i = length(already_handled_data)
  print(length(uris_to_get))
  for(l in r){
    out <- getURI(l)
    uri_output <- c(uri_output,out)
    save(uri_output,file=paste(home_dir,"data/",city,"uri_output_f",as.character(i),".rdata",sep=""))
    i= i +maxi
    print(i)
    print(uri_output[length(uri_output)])
    if(uri_output[length(uri_output)] == ""){
      return("")
    }
    Sys.sleep(60)
  }
  print(uri_output[1])
  block <- sapply(uri_output,function(txt){
    y <- xmlParse(txt, asText=TRUE)
    xmlToList(y)$Block})
  
  nodes$Block <- block
  save(nodes,file=paste(home_dir,"data/",city,"/nodes.rdata",sep=""))
  nodes
}


perform_census_queries <-function(data,queries){
  data$State <- substr(data$Block,1,2)
  data$County <- substr(data$Block,3,5)
  data$Tract <- substr(data$Block,6,11)
  data$BlockGroup <- substr(data$Block,12,12)
  data$BlockNum <- substr(data$Block,12,15)
  
  print(head(data))
  uni_loc <- unique(data[,c("State","County","Tract","BlockGroup")])
  blah <- data.frame(apply(uni_loc,2,as.numeric))
  na_rows <- unique(as.vector(apply(blah,2,function(t){which(is.na(t))})))
  if(length(na_rows) > 0){
    uni_loc <- uni_loc[-na_rows,]
  }
  uni_loc <- ddply(uni_loc,.(State,County,Tract),function(t){paste0(t$BlockGroup,collapse=",")})
  
  get_str <- paste0("http://api.census.gov/data/2011/acs5?key=",CENSUS_API_KEY,"&get=")
  get_str_2 <- "&for=block+group:BLOCK_GROUP&in=state:STATE+county:COUNTY+tract:TRACT"
  q_t_1 <-paste0(queries$terms[1:33],collapse=",")
  q_t_2 <- paste0(queries$terms[34:66],collapse=",")
  i= 0
  census_data_queries <- apply(uni_loc,1,function(t){
    s <- sub("STATE",t[1],get_str_2)
    s <- sub("COUNTY",t[2],s)
    s <- sub("TRACT",t[3],s)
    s <- sub("BLOCK_GROUP",t[4],s)
    s
    s1 <- paste(get_str,q_t_1,s,sep="")
    s2 <- paste(get_str,q_t_2,s,sep="")
    c(s1,s2)
  })
  print(length(census_data_queries))
  returned_q <- lapply(census_data_queries,function(t){
    ###NOTE: Not actually making the queries here
    print(t)
    suppressWarnings(read.csv(t, na.strings=c("-", "**", "***", "(X)", "N", "null"), stringsAsFactors=F))
  })
  returned_q
}

merge_venue_data_with_census_data <- function(venue_data,census_data,city,queries){
  all_1 <- do.call(rbind, census_data[seq(1,length(census_data),by=2)])
  all_1 <- all_1[,-length(all_1)]
  all_1 <- all_1[,!(colnames(all_1) %in% c("state","county","tract","block.group.")) ]
  all_2 <- do.call(rbind, census_data[seq(2,length(census_data),by=2)])
  all_2 <- all_2[,-length(all_2)]
  all <- cbind(all_1,all_2)
  all.df <- data.frame(all)
  all.df$block.group. <- gsub("]","",all.df$block.group.,fixed=T)
  all.df[,1] <- gsub("[","",all.df[,1],fixed=T)
  all.df$X..B08301_017E <- gsub("[","",all.df$X..B08301_017E,fixed=T)
  all.df <- data.frame(apply(all.df,2,as.numeric))
  
  venue_data$State <- as.numeric(substr(venue_data$Block,1,2))
  venue_data$County <- as.numeric(substr(venue_data$Block,3,5))
  venue_data$Tract <- as.numeric(substr(venue_data$Block,6,11))
  venue_data$BlockGroup <- as.numeric(substr(venue_data$Block,12,12))
  venue_data$BlockNum <- as.numeric(substr(venue_data$Block,12,15))
  venue_data <- venue_data[!is.na(venue_data$State),]
  
  z <- merge(venue_data,all.df, by.x=c("State","County","Tract","BlockGroup"),by.y=c("state","county","tract","block.group."))
  z$Block <- as.character(z$Block)
  names(z) <- str_replace(names(z),"X..","")
  names(z) <- sapply(names(z),function(t){ifelse(t %in% queries$terms,as.character(queries[queries$terms==t,"names"]),t)})
   z <- z[,!(names(z) %in% c("Total:",
                           "Not Hispanic or Latino",
                           "Two or more races:  Two races including Some other race",
                           "Two or more races:  Two races excluding Some other race, and three or more races",
                           "Aggregate travel time to work (in minutes):",
                           "Male",
                           "Female",
                           "Gini Index")) ]
  z[is.na(z)] <- 0
  z
}



get_checkin_data <- function(home_dir,city){

  require(lubridate)
  require(stringr)
  data <- read.csv(paste0(home_dir,"data/",city,"/checkin_data.txt"),sep="\t",header=FALSE,stringsAsFactors=FALSE)
  names(data) <- c("DateTime","User","Venue","PlaceCategory", "Longitude","Latitude", "PlaceName")
  df <- str_split_fixed(data$PlaceCategory, "::",2)
  df <- data.frame(topLevel=df[,1], bottomLevel=df[,2])
  data <- cbind(data, df)
  data[data$topLevel == "Colleges & Universities", "topLevel"] <- "College & University"
  data[data$topLevel == "Shops", "topLevel"] <- "Shop & Service"
  data[data$topLevel == "Home, Work, Others", "topLevel"] <- "Homes, Work, Others"
  levels(data$topLevel)[levels(data$topLevel) == "Professional & Other Places"] <- "Professional & Other"
  #data <- data[data$topLevel != "\\N" & data$topLevel !="",]
  data$topLevel <- factor(data$topLevel)
  data$UniqueID <- paste(data$Latitude,data$Longitude,data$PlaceName, data$PlaceCategory,sep="_")
  data$time <- ymd_hms(data$DateTime)
  data$hour <- hour(data$time)
  data$day <- wday(data$time,label=TRUE)
  r <- expand.grid(c(0:23),c("Mon","Tues","Wed","Thurs","Fri","Sat","Sun"))
  data$HourDay <- factor(paste(data$hour,as.character(data$day)),levels=paste(r$Var1,r$Var2),ordered=TRUE)
  data
}
