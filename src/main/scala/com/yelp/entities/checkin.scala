package com.yelp.entities

case class checkin_time(Monday: Map[String, Int],
                        Tuesday: Map[String, Int],
                        Wednesday: Map[String, Int],
                        Thursday: Map[String, Int],
                        Friday: Map[String, Int],
                        Saturday: Map[String, Int],
                        Sunday: Map[String, Int])

case class checkin(business_id: String, time: checkin_time)
