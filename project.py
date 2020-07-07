###Christopher Stein(218202663)

###imports
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.types import DoubleType
import geopandas as gpd
import numpy


###Method to import the Data-Table from local storage
###and just take the important columns
### Country Name, startyear and endyear
def extract_data(spark,startyear,endyear):
    
    co2_data = spark.read.option("inferSchema", "true").option("header", "true").csv(
        "C:/Users/Stone/Desktop/Uni/BigData/Project/API_EN.ATM.CO2E.PC_DS2_en_csv_v2_1217665.csv")
    
    return co2_data.select("Country Name", str(startyear), str(endyear))


###Method to devide the different countries in 3 different groups
###using k-means to their carbon dioxide emission trend
def k_means(combined):
    
    ###sort the values and reset index to find good start points
    combined = combined.sort_values(by=['Diff'])
    combined=combined.reset_index(drop=True)
    
    #get trend-values from Data set
    values = combined['Diff']
    
    ###set start points to first element, median and last element
    green_start = 0
    yellow_start = int(len(values)/2)
    red_start = len(values)-1    
    
   
    ###init averages
    green_average=values[green_start]
    yellow_average=values[yellow_start]
    red_average=values[red_start]
    
    
    
    ###init color lists
    new_color = ["" for x in range(len(values))]
    old_color = []
    
    ###Caluculate as long as countries "switch their color"
    while True:
        ###lists for next round
        greens=[]
        yellows=[]
        reds=[]
        
        ##iterate over all values 
        for i in range(len(values)):
            
            ###check if green is right for that value
            if ((numpy.abs(values[i]-green_average) 
               < numpy.abs(values[i]-yellow_average))
            and    (numpy.abs(values[i]-green_average) 
               < numpy.abs(values[i]-red_average)
               )):
                new_color[i]="green"
                greens.append(values[i])
            
            ###check if yellow is right for that value
            elif (numpy.abs(values[i]-yellow_average) 
               < numpy.abs(values[i]-red_average)):
                 new_color[i]="yellow"
                 yellows.append(values[i])
                 
            ### value is red
            else:
                new_color[i]="red"
                reds.append(values[i])
                
        ### break condition
        if old_color == new_color:
                break
        
        ###old_color for next round
        old_color = new_color
            
        #calulate new Averages
        green_average = sum(greens)/len(greens)
        yellow_average = sum(yellows)/len(yellows)
        red_average = sum(reds)/len(reds)
                
    
    ###add color list to combined dataset
    combined['color']=new_color
        
        
    return combined

###Select years from 1960-2019
startyear=2010
endyear=2011
    
#init spark
sc = SparkContext("local", "BigDataProject")
sc.setLogLevel("ERROR")
spark = SparkSession(sc)

###get data 
result = extract_data(spark,startyear,endyear)

#change Column data type to Double for calculations
result = result.withColumn(str(startyear), result[str(startyear)].cast(DoubleType()))
result = result.withColumn(str(endyear), result[str(endyear)].cast(DoubleType()))

###remove rows which are not full of data
result = result.na.drop()


###get data for world plot
world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
    
###reduce dataset world to name and geometry
world = world[['name', 'geometry']]

###find all names which are not equal in world and data

#for item in data['Country Name']:
 #   world_data_list = world['name'].tolist()
  #  if item in world_data_list:
   #   pass
   # else:
    #  print(item)
    
###remove Anarctica for better view
world = world[ world.name != 'Fr. S. Antarctic Lands' ]
world = world[ world.name != 'Antarctica' ]

### change the name of countries in variable world to name in data for merge
world.replace('Russia', 'Russian Federation', inplace= True)
world.replace('Syria', 'Syrian Arab Republic', inplace= True)
world.replace('United States of America', 'United States', inplace= True)
world.replace('Congo', 'Congo, Rep.', inplace= True)
world.replace('Czechia', 'Czech Republic', inplace= True)
world.replace('Côte d\'Ivoire', 'Cote d\'Ivoire', inplace= True)
world.replace('Yemen', 'Yemen, Rep.', inplace= True)
world.replace('Bahamas', 'Bahamas, The', inplace= True)
world.replace('Bosnia and Herz.', 'Bosnia and Herzegovina', inplace= True)
world.replace('Central African Rep.', 'Central African Republic', inplace= True)
world.replace('Dem. Rep. Congo', 'Congo, Dem. Rep.', inplace= True)
world.replace('Egypt, Arab Rep.', 'Egypt', inplace= True)
world.replace('Eq. Guinea', 'Equatorial Guinea', inplace= True)
world.replace('Iran', 'Iran, Islamic Rep.', inplace= True)
world.replace('Slovakia', 'Slovak Republic', inplace= True)
world.replace('South Korea', 'Korea Rep.', inplace= True)
world.replace('North Korea', 'Korea, Dem. People’s Rep.', inplace= True)
world.replace('Venezuela', 'Venezuela, RB', inplace= True)
world.replace('Dominican Rep.', 'Dominican Republic', inplace= True)
world.replace('Gambia', 'Gambia, The', inplace= True)
world.replace('Solomon Is.', 'Solomon Islands', inplace= True)
world.replace('Brunei', 'Brunei Darussalam', inplace= True)
world.replace('Macedonia', 'North Macedonia', inplace= True)
world.replace('S. Sudan', 'South Sudan', inplace= True)



###migrate data to pandas
data = result.select("*").toPandas()   

###rename column name Country name to name for merge  
data.rename(columns = {"Country Name" : 'name'}, inplace = True)
     
###merge data and world 
combined = world.merge(data, on = 'name')

###calculate the procentage Difference betwenn start- and endyear
combined['Diff']=(combined[str(endyear)]/combined[str(startyear)])*100-100




#PLOT
ax = combined.plot(column = 'Diff', cmap ='Reds',  legend=True,
                   edgecolor='black', linewidth= 0.2)

### set Title
ax.set_title("Percentage change of CO2-emissions per capita \n from "
             +str(startyear)+ " - "+str(endyear))

### remove axis around plotted world
ax.set_axis_off()




###calculate three groups (green, yellow, red)
combined = k_means(combined)

#PLOT2
bx = combined.plot(color=combined['color'],legend=True,
                   edgecolor='black', linewidth= 0.2)

### set Title
bx.set_title(" Grouped by Percentage change of CO2-emissions \n between "
             +str(startyear)+ " - "+str(endyear))


### remove axis around plotted world
bx.set_axis_off()



