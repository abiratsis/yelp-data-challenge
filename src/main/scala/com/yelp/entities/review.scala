package com.yelp.entities

case class review (review_id: String, user_id: String, business_id:String, stars:Short, date: String, text: String, useful: Short, funny: Short, cool: Short)
