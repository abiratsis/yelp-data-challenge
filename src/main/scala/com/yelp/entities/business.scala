package com.yelp.entities

case class businessParking(garage: Boolean, street : Boolean, validated:Boolean, lot: Boolean, valet : Boolean)
case class businessMetadata(RestaurantsTakeOut :Boolean, BusinessParking: businessParking)
case class business(business_id : String,
                     name : String,
                     neighborhood: String,
                     address: String,
                     city: String,
                     state: String,
                     postal_code : String,
                     latitude : Float,
                     longitude : Float,
                     stars : Float,
                     review_count : Int,
                     is_open : Short,
                     attributes : businessMetadata,
                     categories: Array[String],
                     hours : Map[String, String])
