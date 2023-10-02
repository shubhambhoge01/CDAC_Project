#!/usr/bin/env python3


import pyspark
from pyspark.sql.session import SparkSession



if __name__ == "__main__":

    spark = SparkSession \
                .builder \
                .appName("eda_query") \
                .enableHiveSupport()\
                .getOrCreate()

    from pyspark.sql.functions import *
    from pyspark.context import SparkContext
    from pyspark.sql import SQLContext

    from pyspark.sql.functions import regexp_replace
    from pyspark.sql.functions import *
    from pyspark.sql import *
    from pyspark.sql.types import *
    from functools import reduce  
    from pyspark.sql.functions import split

    #### s3 bucket name --> project21-vita 

    ### Reading Business file

    business = spark.read.format("csv") \
                            .option("header", "true") \
                            .option("inferSchema", "true") \
                            .option("quote", "\"")\
                            .option("escape", "\"")\
                            .load("s3://project21-vita/kaggle/business.csv")

    business1 = business.drop("neighborhood")

    business2 = business1.na.fill(value='Edinburgh',subset=["city"])

    business3 = business2.na.fill(value='NV',subset=["State"])

    business4 = business3.na.fill(value='',subset=["postal_code"])

    business5 = business4.na.fill(value=43.651070 , subset=["Latitude"])

    business6 = business5.na.fill(value=-79.347015 , subset=["longitude"])

    business7 = business6.withColumn('state', regexp_replace('state', '6', 'STE'))\
    .withColumn('state', regexp_replace('state', '30', 'OCC'))\
    .withColumn('state', regexp_replace('state', '3', 'NOE'))\
    .withColumn('state', regexp_replace('state', '01', 'Viken'))

    business8 = business7.withColumn('is_open', regexp_replace('is_open', '0', 'close'))\
    .withColumn('is_open', regexp_replace('is_open', '1', 'open'))\
    .withColumn('name', regexp_replace('name', '"', ''))\
    .withColumn('address', regexp_replace('address', '"', ''))

    ### saving file

    spark.sql("create database yelp")

    business8.createOrReplaceTempView("business")

    business8.write.saveAsTable("yelp.business")

    business_restaurant = business8.filter((business8.categories.like("%Pizza%"))|(business8.categories.like("%Restaurants%"))|(business8.categories.like("%Diners%"))|(business8.categories.like("%Coffee & Tea%"))|(business8.categories.like("%Ice Cream%"))|(business8.categories.like("%Juice Bars & Smoothies%"))|(business8.categories.like("%Seafood%"))|(business8.categories.like("%Barbeque%"))|(business8.categories.like("%Donuts%"))|(business8.categories.like("%Thai%"))|(business8.categories.like("%bars%"))|(business8.categories.like("%Sushi Bars%"))|(business8.categories.like("%Ramen%"))|(business8.categories.like("%Taiwanese%"))|(business8.categories.like("%Specialty Food%"))|(business8.categories.like("%Vietnamese%"))|(business8.categories.like("%Sandwiches%"))|(business8.categories.like("%Vegetarian%"))|(business8.categories.like("%Vegan%"))|(business8.categories.like("%Tacos%"))|(business8.categories.like("%American (Traditional)%"))|(business8.categories.like("%Dim Sum%"))|(business8.categories.like("%Breweries%"))|(business8.categories.like("%Wine & Spirits%"))|(business8.categories.like("%Italian%"))|(business8.categories.like("%Persian/Iranian%"))|(business8.categories.like("%Cafes%"))|(business8.categories.like("%Indian%"))|(business8.categories.like("%Delis%"))|(business8.categories.like("%Canadian (New)%"))|(business8.categories.like("%Soul Food%"))|(business8.categories.like("%American (New)%"))|(business8.categories.like("%Asian Fusion%"))|(business8.categories.like("%Cheesesteaks%"))|(business8.categories.like("%soup%"))|(business8.categories.like("%Caterers%"))|(business8.categories.like("%Breakfast & Brunch%"))|(business8.categories.like("%Lounges%"))|(business8.categories.like("%Cocktail Bars%"))|(business8.categories.like("%Champagne Bars%"))|(business8.categories.like("%Mediterranean%"))|(business8.categories.like("%Poutineries%")))

    business_restaurant.createOrReplaceTempView("business_restaurant")

    query_business = spark.sql("select business_id,name,address,city,state,postal_code,latitude,longitude,stars,review_count,is_open, if(categories like '%Italian%','Italian',if(categories like '%Mexican%','Mexican', if(categories like '%Japanese%','Japanese',if(categories like '%Indian%','Indian',if(categories like '%Chinese%','Chinese',if(categories like '%American%','American',if(categories like '%Vegetarian%','Vegetarian',if(categories like '%Korean%','Korean',if(categories like '%Vietnamese%','Vietnamese',if(categories like '%Barbecue%','Barbecue',if(categories like '%Thai%','Thai',if(categories like '%Seafood%','Seafood',if(categories like '%Turkish%','Turkish',if(categories like '%Korean%','Korean',if(categories like '%Indonesian%','Indonesian',if(categories like '%Swedish%','Swedish',if(categories like '%Asian%','Asian',if(categories like '%Irish%','Irish',if(categories like '%Russian%','Russian',if(categories like '%Spanish%','Spanish',if(categories like '%French%','French',if(categories like '%Fast Food%','Fast Food','Other')))))))))))))))))))))) as Type_of_Cuisines_in_Restaurants from business_restaurant")

    query_business.write.saveAsTable("yelp.business_restaurant_table")


    ### Reading Review

    review_schema = StructType([
        StructField("review_id", StringType()),
        StructField("user_id", StringType()),
        StructField("business_id", StringType()),
        StructField("stars", FloatType()),
        StructField("date", StringType()),
        StructField("text", StringType()),
        StructField("useful", IntegerType()),
        StructField("funny", IntegerType()),
        StructField("cool", IntegerType())
        ])


    review= spark.read.format("csv") \
                            .option("header", "true") \
                            .schema(review_schema) \
                            .option("quote", "\"")\
                            .option("escape", "\"")\
                            .option("dateFormat", "yyyy-MM-dd HH:mm:ss")\
                            .option("multiLine",'true')\
                            .load("s3://project21-vita/kaggle/review.csv")

    ### saving file

    review.createOrReplaceTempView("review")

    review_restaurant = spark.sql("select b.review_id,b.user_id,a.name,b.business_id,b.stars,b.date,b.text,b.useful,b.funny,b.cool from business_restaurant as a inner join review as b on a.business_id==b.business_id")

    review_restaurant.createOrReplaceTempView("review_restaurant")

    ### Reading Checkin

    checkin = spark.read.format("csv") \
                            .option("header", "true") \
                            .option("inferSchema","true")\
                            .load("s3://project21-vita/kaggle/checkin.csv")

    ### saving file

    checkin.createOrReplaceTempView("checkin")

    checkin_restaurant = spark.sql("select a.name,b.business_id, b.weekday, b.hour, b.checkins from business_restaurant as a inner join checkin as b on a.business_id=b.business_id")

    checkin_restaurant.write.saveAsTable("yelp.checkin_restaurant_table")

    ### Reading Tip

    tip_schema = StructType([
        StructField("text", StringType()),
        StructField("date", StringType()),
        StructField("likes", IntegerType()),
        StructField("business_id", StringType()),
        StructField("user_id", StringType()),
    ])

    tip = spark.read.format("csv") \
                            .option("header", "true") \
                            .schema(tip_schema)\
                            .option("quote", "\"")\
                            .option("escape", "\"")\
                            .option("multiLine",'true')\
                            .load("s3://project21-vita/kaggle/tip.csv")

    tip1 = tip.na.fill(value='',subset=["text"])


    ### saving file

    tip1.createOrReplaceTempView("tip")

    tip_restaurant =  spark.sql("select a.text,a.date,a.likes,a.business_id,a.user_id from tip as a inner join business_restaurant as b on a.business_id==b.business_id")


    ### Reading hours file

    hours = spark.read.format("csv") \
                            .option("header", "true") \
                            .option("inferSchema","true")\
                            .load("s3://project21-vita/kaggle/hours.csv")

    hours1 = hours.withColumn('monday', regexp_replace('monday', 'None', ''))\
    .withColumn('tuesday', regexp_replace('tuesday', 'None', ''))\
    .withColumn('wednesday', regexp_replace('wednesday', 'None', ''))\
    .withColumn('thursday', regexp_replace('thursday', 'None', ''))\
    .withColumn('friday', regexp_replace('friday', 'None', ''))\
    .withColumn('saturday', regexp_replace('saturday', 'None', ''))\
    .withColumn('sunday', regexp_replace('sunday', 'None', ''))

    hours2 = hours1.withColumn('Mon_opening', split(hours1['monday'], '-').getItem(0))\
                                    .withColumn('Mon_closing', split(hours1['monday'], '-').getItem(1))\
                                    .withColumn('Tue_opening', split(hours1['tuesday'], '-').getItem(0))\
                                    .withColumn('Tue_closing', split(hours1['tuesday'], '-').getItem(1))\
                                    .withColumn('Wed_opening', split(hours1['wednesday'], '-').getItem(0))\
                                    .withColumn('Wed_closing', split(hours1['wednesday'], '-').getItem(1))\
                                    .withColumn('Thu_opening', split(hours1['thursday'], '-').getItem(0))\
                                    .withColumn('Thu_closing', split(hours1['thursday'], '-').getItem(1))\
                                    .withColumn('fri_opening', split(hours1['friday'], '-').getItem(0))\
                                    .withColumn('fri_closing', split(hours1['friday'], '-').getItem(1))\
                                    .withColumn('Sat_opening', split(hours1['saturday'], '-').getItem(0))\
                                    .withColumn('Sat_closing', split(hours1['saturday'], '-').getItem(1))\
                                    .withColumn('Sun_opening', split(hours1['sunday'], '-').getItem(0))\
                                    .withColumn('Sun_closing', split(hours1['sunday'], '-').getItem(1))\


    hours3 = hours2.drop('monday','tuesday','wednesday','thursday','friday','saturday','sunday')


    ### saving file

    hours1.createOrReplaceTempView("hours1")

    hours_restaurant1 = spark.sql("select b.business_id,b.monday,b.tuesday,b.wednesday,b.thursday,b.friday,b.saturday,b.sunday from business_restaurant as a inner join hours1 as b on a.business_id=b.business_id")

    hours_restaurant1.write.saveAsTable("yelp.hours_restaurant1_table")

    hours3.createOrReplaceTempView("hours")

    hours_restaurant = spark.sql("select b.business_id,b.Mon_opening,b.Mon_closing,b.Tue_opening,b.Tue_closing,b.Wed_opening,b.Wed_closing,b.Thu_opening,b.Thu_closing,b.fri_opening,b.fri_closing,b.Sat_opening,b.Sat_closing,b.Sun_opening,b.Sun_closing from business_restaurant as a inner join hours as b on a.business_id=b.business_id")


    ### Reading user

    user = spark.read.format("csv") \
                            .option("header", "true") \
                            .option("inferSchema", "true") \
                            .load("s3://project21-vita/kaggle/user.csv")

    user1 = user.withColumn("Yelping_since", col("Yelping_since").cast("String"))

    user2 = user1.na.fill(value='Unknown',subset=["name"])

    user3 = user2.withColumn('name', regexp_replace('name', 'None', 'Unknown'))\
            .withColumn('elite', regexp_replace('elite', 'None', ''))        

    user4 = user3.drop('friends','compliment_hot',
    'compliment_more',
    'compliment_profile',
    'compliment_cute',
    'compliment_list',
    'compliment_note',
    'compliment_plain',
    'compliment_cool',
    'compliment_funny',
    'compliment_writer',
    'compliment_photos')

    user5 = user4.withColumn("Yelping_date", split(col("Yelping_since"), " ").getItem(0)).withColumn("col2", split(col("Yelping_since"), " ").getItem(1))

    user6 = user5.drop('Yelping_since','col2')

    user7 = user6.withColumnRenamed('Yelping_date', 'Yelping_since') # renaming  column yelping date as yelping_since

    user8 = user7.select('user_id','name','review_count','Yelping_since','useful','funny','cool','elite','average_stars','fans')

    user8.createOrReplaceTempView("user")

    user_review_restaurant = spark.sql("select a.user_id,a.name,a.review_count,a.Yelping_since,a.fans,a.useful,a.funny,a.cool,a.elite,a.average_stars from user as a inner join review_restaurant as b on a.user_id==b.user_id ")

    user_review_restaurant.createOrReplaceTempView("user_review_restaurant")


    ### Reading attribute


    attribute = spark.read.format("csv") \
                            .option("header", "true") \
                            .option("quote", "\"")\
                            .option("escape", "\"")\
                            .option("multiLine",'true')\
                            .load("s3://project21-vita/kaggle/attribute.csv")

    attribute1 = attribute.drop('DietaryRestrictions_kosher', 'DietaryRestrictions_halal','DietaryRestrictions_soy-free',
        'DietaryRestrictions_vegetarian','RestaurantsCounterService','DietaryRestrictions_gluten-free',
        'DietaryRestrictions_dairy-free','DietaryRestrictions_vegan', 'BYOBCorkage', 'BYOB','CoatCheck',
        'Ambience_hipster','Ambience_divey','Ambience_intimate','Ambience_trendy','Ambience_upscale','HairSpecializesIn_asian','AcceptsInsurance','ByAppointmentOnly',
        'BusinessAcceptsCreditCards','BusinessParking_garage','BusinessParking_street','BusinessParking_validated','BusinessParking_lot','BusinessParking_valet',
        'HairSpecializesIn_coloring','HairSpecializesIn_africanamerican','HairSpecializesIn_curly','HairSpecializesIn_perms','HairSpecializesIn_kids','HairSpecializesIn_extensions',
        'HairSpecializesIn_straightperms','RestaurantsPriceRange2','Music_dj','Music_karaoke','Music_video','Music_jukebox','Ambience_touristy','AgesAllowed','BestNights_monday','BestNights_tuesday','BestNights_wednesday','BestNights_thursday','GoodForMeal_brunch')

    attribute2 = attribute1.withColumn('GoodForKids', regexp_replace('GoodForKids', 'Na', 'False'))\
                            .withColumn('WheelchairAccessible', regexp_replace('WheelchairAccessible', 'Na', 'False'))\
                            .withColumn('BikeParking', regexp_replace('BikeParking', 'Na', 'False'))\
                            .withColumn('Alcohol', regexp_replace('Alcohol', 'Na', 'False'))\
                            .withColumn('HasTV', regexp_replace('HasTV', 'Na', 'False'))\
                            .withColumn('NoiseLevel', regexp_replace('NoiseLevel', 'Na', 'False'))\
                            .withColumn('RestaurantsAttire', regexp_replace('RestaurantsAttire', 'Na', 'False'))\
                            .withColumn('Music_background_music', regexp_replace('Music_background_music', 'Na', 'False'))\
                            .withColumn('Music_no_music', regexp_replace('Music_no_music', 'Na', 'False'))\
                            .withColumn('Music_live', regexp_replace('Music_live', 'Na', 'False'))\
                            .withColumn('Ambience_romantic', regexp_replace('Ambience_romantic', 'Na', 'False'))\
                            .withColumn('Ambience_classy', regexp_replace('Ambience_classy', 'Na', 'False'))\
                            .withColumn('Ambience_casual', regexp_replace('Ambience_casual', 'Na', 'False'))\
                            .withColumn('RestaurantsGoodForGroups', regexp_replace('RestaurantsGoodForGroups', 'Na', 'False'))\
                            .withColumn('Caters', regexp_replace('Caters', 'Na', 'False'))\
                            .withColumn('WiFi', regexp_replace('WiFi', 'Na', 'False'))\
                            .withColumn('RestaurantsReservations', regexp_replace('RestaurantsReservations', 'Na', 'False'))\
                            .withColumn('RestaurantsTakeOut', regexp_replace('RestaurantsTakeOut', 'Na', 'False'))\
                            .withColumn('HappyHour', regexp_replace('HappyHour', 'Na', 'False'))\
                            .withColumn('GoodForDancing', regexp_replace('GoodForDancing', 'Na', 'False'))\
                            .withColumn('RestaurantsTableService', regexp_replace('RestaurantsTableService', 'Na', 'False'))\
                            .withColumn('OutdoorSeating', regexp_replace('OutdoorSeating', 'Na', 'False'))\
                            .withColumn('RestaurantsDelivery', regexp_replace('RestaurantsDelivery', 'Na', 'False'))\
                            .withColumn('BestNights_friday', regexp_replace('BestNights_friday', 'Na', 'False'))\
                            .withColumn('BestNights_sunday', regexp_replace('BestNights_sunday', 'Na', 'False'))\
                            .withColumn('BestNights_saturday', regexp_replace('BestNights_saturday', 'Na', 'False'))\
                            .withColumn('GoodForMeal_dessert', regexp_replace('GoodForMeal_dessert', 'Na', 'False'))\
                            .withColumn('GoodForMeal_latenight', regexp_replace('GoodForMeal_latenight', 'Na', 'False'))\
                            .withColumn('GoodForMeal_lunch', regexp_replace('GoodForMeal_lunch', 'Na', 'False'))\
                            .withColumn('GoodForMeal_dinner', regexp_replace('GoodForMeal_dinner', 'Na', 'False'))\
                            .withColumn('GoodForMeal_breakfast', regexp_replace('GoodForMeal_breakfast', 'Na', 'False'))\
                            .withColumn('Smoking', regexp_replace('Smoking', 'Na', 'False'))\
                            .withColumn('DriveThru', regexp_replace('DriveThru', 'Na', 'False'))\
                            .withColumn('DogsAllowed', regexp_replace('DogsAllowed', 'Na', 'False'))\
                            .withColumn('BusinessAcceptsBitcoin', regexp_replace('BusinessAcceptsBitcoin', 'Na', 'False'))\
                            .withColumn('Open24Hours', regexp_replace('Open24Hours', 'Na', 'False'))\
                            .withColumn('Corkage', regexp_replace('Corkage', 'Na', 'False'))

    ### saving file

    attribute.createOrReplaceTempView("attribute")

    attribute_restaurant = spark.sql("select b.business_id,a.name,b.GoodForKids,b.WheelchairAccessible,b.BikeParking,b.Alcohol,\
                                    b.HasTV,b.NoiseLevel,b.RestaurantsAttire,b.Music_background_music,b.Music_no_music,\
                                    b.Music_live,b.Ambience_romantic,b.Ambience_classy,b.Ambience_casual,b.RestaurantsGoodForGroups,\
                                    b.Caters,b.WiFi,b.RestaurantsReservations,b.RestaurantsTakeOut,b.HappyHour,b.GoodForDancing,b.RestaurantsTableService,\
                                    b.OutdoorSeating,b.RestaurantsDelivery,b.BestNights_friday,b.BestNights_sunday,b.BestNights_saturday,b.GoodForMeal_dessert,\
                                    b.GoodForMeal_latenight,b.GoodForMeal_lunch,b.GoodForMeal_dinner,b.GoodForMeal_breakfast,b.Smoking,b.DriveThru,b.DogsAllowed,\
                                    b.BusinessAcceptsBitcoin,b.Open24Hours,b.Corkage\
                                    from business_restaurant as a inner join attribute as b on a.business_id==b.business_id")

    ### NLP

    nlp = spark.read.format("csv") \
                            .option("header", "true") \
                            .option("inferSchema", "true") \
                            .load("s3://project21-vita/nlp_on_review/nlp_review.csv")

    nlp.columns

    nlp.createOrReplaceTempView("nlp")

    nlp_review = spark.sql("select a.business_id,a.review_id,a.response from nlp a inner join business_restaurant b on a.business_id==b.business_id ")

    nlp_review.write.saveAsTable("yelp.nlp_review")

    ### Starting Query

    query44 = spark.sql("select business_id,name,year(date) year,count(*) total_count from review_restaurant group by business_id,name,year(date) ")

    query44.write.saveAsTable("yelp.review_restaurant_table")

    ### 4.	average rating over the year of a particular restaurant

    query4 = spark.sql("select name as Business_name,year(date) as year,round(avg(stars),2) as average_rating from review_restaurant group by year(date),name order by year(date) asc")

    query4.write.saveAsTable("yelp.query4")

    ### 31. Total Businesses on yelp based on categories

    query31 = spark.sql("select count(business_id) as Total_Businesses, if(categories like'%Restaurant%','Restaurants',if(categories like'%Health%','Health & medical',IF(categories like'%Shopping%','Shopping',if(categories like'%Real Estate%','Real Estate',if(categories like'%Home Services%','Home Services', if(categories like'%Financial Services%','Financial Services',if(categories like'%Bars%','Bars',if(categories like'%Food %','Food',if(categories like'%Beauty & Spas%','Beauty & Spas',if(categories like'%Active Life%','Active Life',if(categories like'%Arts & Entertainment%','Arts & Entertainment',if(categories like'%Automotive%','Automotive',if(categories like'%Event Planning & Services%','Event Planning & Services',if(categories like'%Security Systems%','Security Systems',if(categories like'%Education%','Education',if(categories like'%Hotels & Travel%','Hotels & Travel',if(categories like'%Local Services%','Local Services',if(categories like'%Mass Media%','Mass Media',if(categories like'%Pet%','Pets',if(categories like'%Religious Organizations%','Religious Organizations',if(categories like'%Professional Services%','Professional Services',if(categories like'%Public Services & Government%','Public Services & Government','Others'))))))))))))))))))))))as Business_categories from business group by Business_categories ")

    query31.write.saveAsTable("yelp.query31")

    ### 36. top restaurants according to city(use text box when enter city name we get top 10 restaurants in that city according to stars)

    query36 = spark.sql("select * from (select City ,name,stars , row_number() over(partition by city order by stars desc ) as row from business_restaurant ) as t where row between 1 and 10 " )

    query36.write.saveAsTable("yelp.query36")

    ###  43. top restaurants according to city(use text box when enter city name we get bottom 10 restaurants in that city according to stars)

    query43 = spark.sql("select * from (select City ,name,stars , row_number() over(partition by city order by stars asc ) as row from business_restaurant ) as t where row between 1 and 10 " )

    query43.write.saveAsTable("yelp.query43")

    ### 37. How many elite members or non elite members are their in dataset

    query37 = spark.sql("SELECT  ( SELECT  COUNT(*) FROM  user_review_restaurant WHERE elite not like '') AS elite_member,\
            (SELECT  COUNT(*) FROM  user_review_restaurant WHERE elite like '') AS Non_elite_member")

    query37.write.saveAsTable("yelp.query37")

    ### 41. total users registered on yelp(for all categories not for restaurant only)

    query41 = spark.sql("select year(Yelping_since) as year,count(1) as Total_users from user group by year(Yelping_since) order by year asc")

    query41.write.saveAsTable("yelp.query41")
