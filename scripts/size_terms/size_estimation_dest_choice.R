library(dplyr)
# for mapping places that have people 
# going to them, but size variables are zero:
library(sf)
library(leaflet)

#This script is prepping the data to be used in this code below
# The instrctions for prepping the data are as follows:

# input.csv is a comma separated values file with one text header row,
# one row for each zone, and the following data columns,
# which must be in order, but can be named differently:
# -------------------------------------------------------------------------------------
# zone: zone number
# demand: the total number of observations in the market segment which chose the zone
# sizevar1: size variable 1
# sizevar2: size variable 2
# .
# .
# .
# sizevarN: size variable N
#
# The minimum number of size variables is 2.
# The maximum is arbitrary.
# Each variable will be used in estimation.
# Each demand and size variable must be non-negative
#
# convergence code of 0 means convergence has been achieved
# for info about other convergence codes type ?optim at an R prompt
#
# output of script is coeficients and standard errors reported to console
#
# Notes:
# 
# 1) There must be at least one zone with no size term, or else the line:
#     estimdata <- estimdata[-zerosize,] 
# throws an error
#
# 2) The script reports coefficients that are the natural log of the size term coefficients.
#
# 3) The standard errors report whether the coefficient is significantly less than or greater than the coefficient for the first variable (which is one).  
#    If you change which variable is first, the results of the test will change.  If the result is significantly negative, you can probably exclude it from the model.





loglik <- function(pars,zoneobs){
  nzones <- nrow(zoneobs)
  nitems <- ncol(zoneobs)
  cursize <- zoneobs[,3] + t( exp(pars) %*% t(zoneobs[,4:nitems]) )
  logsum = log(sum(cursize))
  L = sum( zoneobs[,2] * ( log(cursize) - logsum ) )
  return(-L)
}


# this function returns a list of zones with trips going to them but no size variables avlues

validate_inputs <-function(estimdata){
  
  
  nzones <- nrow(estimdata)
  nitems <- ncol(estimdata)
  
  cat("\n")
  cat("\n")
  cat("Num. zones: ", nzones,"\n")
  cat("Num. size variables: ", nitems-2,"\n")
  cat("Num. observations: ", sum(estimdata[,2]), "\n")
  cat("\n")
  
  cat("Validating input data...")
  cat("\n")
  
  obszerosize <- c()
  zerosize <- c()
  for (i in 1:nzones){
    if (sum(estimdata[i,3:nitems]) <= 0){
      #cat("Sum size terms for row ",i," is less than or equal to 0\n")
      zerosize <- c(zerosize,i)
      if( estimdata[i,2] > 0){
        obszerosize <- c(obszerosize,i)
      }
    }
  }
  
  if (length(obszerosize) > 0){
    cat("The following zones have observed choices but zero size and will be ignored:","\n")
    print(estimdata[obszerosize,])
    cat("\n")
    for (i in 1:10000000){}
  }
  return(estimdata[obszerosize,])
}


estimate_size_terms<- function(estimdata, outfilename='size_coeff.txt', all_lu_vars){
  nzones <- nrow(estimdata)
  nitems <- ncol(estimdata)
  cat("\n")
  cat("\n")
  cat("Num. zones: ", nzones,"\n")
  cat("Num. size variables: ", nitems-2,"\n")
  cat("Num. observations: ", sum(estimdata[,2]), "\n")
  cat("\n")
  
  cat("Validating input data...")
  cat("\n")
  
  obszerosize <- c()
  zerosize <- c()
  for (i in 1:nzones){
    if (sum(estimdata[i,3:nitems]) <= 0){
      #cat("Sum size terms for row ",i," is less than or equal to 0\n")
      zerosize <- c(zerosize,i)
      if( estimdata[i,2] > 0){
        obszerosize <- c(obszerosize,i)
      }
    }
  }
  
  if (length(obszerosize) > 0){
    cat("The following zones have observed choices but zero size and will be ignored:","\n")
    print(estimdata[obszerosize,])
    cat("\n")
  }
  
  estimdata <- estimdata[-zerosize,]
  startvals <- rep(0,nitems-3)
  
  cat("Maximizing Log-Likelihood...\n")
  p <- optim(startvals,loglik,zoneobs=estimdata,hessian=TRUE,control=list(trace=1, maxit=5000))
  cat("Convergence code: ",p$convergence,"\n")
  
  cvar <- solve(p$hessian)
  stderr <- sqrt(diag(cvar))
  #rewrite this; unnecessarily complicated, just making it work
  tempoutfile<-'outputs/temp_size.txt'
  sink(tempoutfile)
  cat("Variable Coefficient SE\n")
  cat(sprintf(names(estimdata)[3]),sprintf("%f",1),sprintf("%f",0),"\n")
  for (i in 4:nitems){
    cat(sprintf(names(estimdata)[i]),sprintf("%f",exp(p$par[i-3])),sprintf("%f",stderr[i-3]),"\n")
    
  }
  sink()
  coeff_small<-fread(tempoutfile)
  # fill in zeros for variables you don't use

  coeff_all<- all_lu_vars%>%left_join(coeff_small)%>% replace(is.na(.), 0)%>%select(Variable, Coefficient)%>%
  pivot_wider(names_from=Variable, values_from=Coefficient)
  
  print(coeff_all)

  write.csv(coeff_all, outfilename)
}



map_trips_no_stuff<- function(zone.lyr, pal, map.lat, map.lon, map.zoom, legend.title, legend.subtitle){
  
  trips_nostuff_map <- 
        leaflet::leaflet() %>%
        leaflet::addMapPane(name = "polygons", zIndex = 410) %>%
        leaflet::addMapPane(name = "maplabels", zIndex = 500) %>% # higher zIndex rendered on top
        
        leaflet::addProviderTiles("CartoDB.VoyagerNoLabels") %>%
        leaflet::addProviderTiles("CartoDB.VoyagerOnlyLabels",
                                  options = leaflet::leafletOptions(pane = "maplabels"),
                                  group = "Labels") %>%
        
        leaflet::addEasyButton(leaflet::easyButton(icon="fa-globe",
                                                   title="Region",
                                                   onClick=leaflet::JS("function(btn, map){map.setView([47.615,-122.257],8.5); }"))) %>% # this needs to be not hard-coded)
        leaflet::addPolygons(data=zone.lyr,
                             fillOpacity = 0.7,
                             fillColor = pal(zone.lyr$num_trips),
                             weight = 1.0,
                             color = "#BCBEC0",
                             group="trips",
                             opacity = 0,
                             stroke=FALSE,
                             options = leaflet::leafletOptions(pane = "polygons"),
                             dashArray = "",
                             highlight = leaflet::highlightOptions(
                               weight =5,
                               color = "76787A",
                               dashArray ="",
                               fillOpacity = 0.7,
                               bringToFront = TRUE)) %>%
        
        leaflet::addLegend(pal = pal,
                           values = zone.lyr$num_trips,
                           position = "bottomright",
                           title = paste(legend.title, '<br>', legend.subtitle)) %>%
        
        leaflet::addLayersControl(baseGroups = "CartoDB.VoyagerNoLabels",
                                  overlayGroups = c("Labels", "trips"))%>%
        
        leaflet::setView(lng=map.lon, lat=map.lat, zoom=map.zoom)
    
    return (trips_nostuff_map)
  }
