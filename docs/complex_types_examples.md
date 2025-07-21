# CQLite Complex Types: Real-World Examples

## Overview

This document provides practical, real-world examples demonstrating how to use CQLite's complex types effectively. All examples use actual production-style data and demonstrate proven patterns.

## E-commerce Platform Examples

### Product Catalog with Variants

```rust
use cqlite_core::Value;
use std::collections::HashMap;

async fn create_product_with_variants() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::new("./ecommerce_data").await?;
    
    // Product variant UDT
    let mut variant1_fields = HashMap::new();
    variant1_fields.insert("sku".to_string(), Value::Text("TSHIRT-RED-M".to_string()));
    variant1_fields.insert("color".to_string(), Value::Text("Red".to_string()));
    variant1_fields.insert("size".to_string(), Value::Text("Medium".to_string()));
    variant1_fields.insert("price".to_string(), Value::Float(29.99));
    variant1_fields.insert("stock_quantity".to_string(), Value::Integer(15));
    variant1_fields.insert("weight_grams".to_string(), Value::Integer(200));
    
    let variant1 = Value::Udt("ProductVariant".to_string(), variant1_fields);
    
    let mut variant2_fields = HashMap::new();
    variant2_fields.insert("sku".to_string(), Value::Text("TSHIRT-BLUE-L".to_string()));
    variant2_fields.insert("color".to_string(), Value::Text("Blue".to_string()));
    variant2_fields.insert("size".to_string(), Value::Text("Large".to_string()));
    variant2_fields.insert("price".to_string(), Value::Float(29.99));
    variant2_fields.insert("stock_quantity".to_string(), Value::Integer(8));
    variant2_fields.insert("weight_grams".to_string(), Value::Integer(220));
    
    let variant2 = Value::Udt("ProductVariant".to_string(), variant2_fields);
    
    // Product attributes map
    let mut attributes = Vec::new();
    attributes.push((Value::Text("material".to_string()), Value::Text("100% Cotton".to_string())));
    attributes.push((Value::Text("care_instructions".to_string()), Value::Text("Machine wash cold".to_string())));
    attributes.push((Value::Text("country_of_origin".to_string()), Value::Text("Vietnam".to_string())));
    attributes.push((Value::Text("brand".to_string()), Value::Text("EcoWear".to_string())));
    
    // Category tags as set
    let category_tags = Value::Set(vec![
        Value::Text("clothing".to_string()),
        Value::Text("casual".to_string()),
        Value::Text("eco-friendly".to_string()),
        Value::Text("cotton".to_string()),
    ]);
    
    // Image URLs as list (order matters for display)
    let image_urls = Value::List(vec![
        Value::Text("https://cdn.example.com/products/tshirt-main.jpg".to_string()),
        Value::Text("https://cdn.example.com/products/tshirt-detail1.jpg".to_string()),
        Value::Text("https://cdn.example.com/products/tshirt-detail2.jpg".to_string()),
        Value::Text("https://cdn.example.com/products/tshirt-model.jpg".to_string()),
    ]);
    
    // Size chart as map
    let mut size_chart = Vec::new();
    size_chart.push((Value::Text("XS".to_string()), Value::Text("Chest: 32-34 inches".to_string())));
    size_chart.push((Value::Text("S".to_string()), Value::Text("Chest: 34-36 inches".to_string())));
    size_chart.push((Value::Text("M".to_string()), Value::Text("Chest: 36-38 inches".to_string())));
    size_chart.push((Value::Text("L".to_string()), Value::Text("Chest: 38-40 inches".to_string())));
    size_chart.push((Value::Text("XL".to_string()), Value::Text("Chest: 40-42 inches".to_string())));
    
    // Create the complete product record
    let mut product_row = HashMap::new();
    product_row.insert("product_id".to_string(), Value::Uuid([1; 16]));
    product_row.insert("name".to_string(), Value::Text("Eco-Friendly Cotton T-Shirt".to_string()));
    product_row.insert("description".to_string(), Value::Text(
        "Comfortable, sustainable t-shirt made from 100% organic cotton. Perfect for everyday wear."
            .to_string()
    ));
    product_row.insert("variants".to_string(), Value::List(vec![variant1, variant2]));
    product_row.insert("attributes".to_string(), Value::Map(attributes));
    product_row.insert("category_tags".to_string(), category_tags);
    product_row.insert("image_urls".to_string(), image_urls);
    product_row.insert("size_chart".to_string(), Value::Map(size_chart));
    product_row.insert("base_price".to_string(), Value::Float(29.99));
    product_row.insert("created_at".to_string(), Value::Timestamp(1640995200000000));
    product_row.insert("updated_at".to_string(), Value::Timestamp(1640995200000000));
    product_row.insert("active".to_string(), Value::Boolean(true));
    
    db.insert("products", product_row).await?;
    println!("✅ Product with variants created successfully");
    Ok(())
}
```

### Shopping Cart with Complex Items

```rust
async fn create_shopping_cart() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::new("./ecommerce_data").await?;
    
    // Cart item UDT with customizations
    let mut item1_customizations = Vec::new();
    item1_customizations.push((Value::Text("engraving".to_string()), Value::Text("Happy Birthday!".to_string())));
    item1_customizations.push((Value::Text("gift_wrap".to_string()), Value::Boolean(true)));
    
    let mut item1_fields = HashMap::new();
    item1_fields.insert("product_id".to_string(), Value::Uuid([10; 16]));
    item1_fields.insert("variant_sku".to_string(), Value::Text("MUG-CERAMIC-BLUE".to_string()));
    item1_fields.insert("quantity".to_string(), Value::Integer(2));
    item1_fields.insert("unit_price".to_string(), Value::Float(15.99));
    item1_fields.insert("customizations".to_string(), Value::Map(item1_customizations));
    item1_fields.insert("added_at".to_string(), Value::Timestamp(1640995200000000));
    
    let cart_item1 = Value::Udt("CartItem".to_string(), item1_fields);
    
    // Simple cart item without customizations
    let mut item2_fields = HashMap::new();
    item2_fields.insert("product_id".to_string(), Value::Uuid([11; 16]));
    item2_fields.insert("variant_sku".to_string(), Value::Text("NOTEBOOK-A5-LINED".to_string()));
    item2_fields.insert("quantity".to_string(), Value::Integer(1));
    item2_fields.insert("unit_price".to_string(), Value::Float(12.50));
    item2_fields.insert("customizations".to_string(), Value::Map(vec![])); // Empty map
    item2_fields.insert("added_at".to_string(), Value::Timestamp(1640995800000000));
    
    let cart_item2 = Value::Udt("CartItem".to_string(), item2_fields);
    
    // Applied discounts as list of discount codes
    let applied_discounts = Value::List(vec![
        Value::Text("WELCOME10".to_string()),
        Value::Text("FIRSTORDER".to_string()),
    ]);
    
    // Shipping options with pricing
    let mut shipping_options = Vec::new();
    shipping_options.push((Value::Text("standard".to_string()), Value::Float(5.99)));
    shipping_options.push((Value::Text("express".to_string()), Value::Float(12.99)));
    shipping_options.push((Value::Text("overnight".to_string()), Value::Float(24.99)));
    
    // Cart totals as frozen tuple (immutable calculation result)
    let cart_totals = Value::Frozen(Box::new(Value::Tuple(vec![
        Value::Float(44.48),  // subtotal
        Value::Float(4.45),   // tax
        Value::Float(5.99),   // shipping
        Value::Float(54.92),  // total
    ])));
    
    let mut cart_row = HashMap::new();
    cart_row.insert("cart_id".to_string(), Value::Uuid([20; 16]));
    cart_row.insert("user_id".to_string(), Value::Uuid([21; 16]));
    cart_row.insert("items".to_string(), Value::List(vec![cart_item1, cart_item2]));
    cart_row.insert("applied_discounts".to_string(), applied_discounts);
    cart_row.insert("shipping_options".to_string(), Value::Map(shipping_options));
    cart_row.insert("selected_shipping".to_string(), Value::Text("standard".to_string()));
    cart_row.insert("cart_totals".to_string(), cart_totals);
    cart_row.insert("created_at".to_string(), Value::Timestamp(1640995200000000));
    cart_row.insert("updated_at".to_string(), Value::Timestamp(1640995800000000));
    cart_row.insert("expires_at".to_string(), Value::Timestamp(1641081600000000)); // 24 hours later
    
    db.insert("shopping_carts", cart_row).await?;
    println!("🛒 Shopping cart created successfully");
    Ok(())
}
```

## Social Media Platform Examples

### User Profile with Rich Content

```rust
async fn create_user_profile() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::new("./social_data").await?;
    
    // Social links UDT
    let mut social_links_fields = HashMap::new();
    social_links_fields.insert("platform".to_string(), Value::Text("twitter".to_string()));
    social_links_fields.insert("username".to_string(), Value::Text("@johndoe".to_string()));
    social_links_fields.insert("url".to_string(), Value::Text("https://twitter.com/johndoe".to_string()));
    social_links_fields.insert("verified".to_string(), Value::Boolean(true));
    
    let twitter_link = Value::Udt("SocialLink".to_string(), social_links_fields);
    
    let mut instagram_fields = HashMap::new();
    instagram_fields.insert("platform".to_string(), Value::Text("instagram".to_string()));
    instagram_fields.insert("username".to_string(), Value::Text("@johndoe_photos".to_string()));
    instagram_fields.insert("url".to_string(), Value::Text("https://instagram.com/johndoe_photos".to_string()));
    instagram_fields.insert("verified".to_string(), Value::Boolean(false));
    
    let instagram_link = Value::Udt("SocialLink".to_string(), instagram_fields);
    
    // Privacy settings as map
    let mut privacy_settings = Vec::new();
    privacy_settings.push((Value::Text("profile_visibility".to_string()), Value::Text("public".to_string())));
    privacy_settings.push((Value::Text("posts_visibility".to_string()), Value::Text("friends".to_string())));
    privacy_settings.push((Value::Text("contact_info_visible".to_string()), Value::Boolean(false)));
    privacy_settings.push((Value::Text("allow_messages".to_string()), Value::Boolean(true)));
    privacy_settings.push((Value::Text("show_online_status".to_string()), Value::Boolean(false)));
    
    // Interests as tags
    let interests = Value::Set(vec![
        Value::Text("photography".to_string()),
        Value::Text("travel".to_string()),
        Value::Text("technology".to_string()),
        Value::Text("cooking".to_string()),
        Value::Text("hiking".to_string()),
        Value::Text("music".to_string()),
    ]);
    
    // Recent activity as list (chronological order)
    let recent_activity = Value::List(vec![
        Value::Text("posted_photo".to_string()),
        Value::Text("liked_post".to_string()),
        Value::Text("commented".to_string()),
        Value::Text("shared_article".to_string()),
        Value::Text("updated_profile".to_string()),
    ]);
    
    // Location data as tuple (lat, lng, city, country)
    let location_data = Value::Tuple(vec![
        Value::Float(37.7749),              // latitude
        Value::Float(-122.4194),            // longitude
        Value::Text("San Francisco".to_string()),
        Value::Text("United States".to_string()),
    ]);
    
    // Follower statistics as frozen map (immutable metrics)
    let mut follower_stats = Vec::new();
    follower_stats.push((Value::Text("followers_count".to_string()), Value::Integer(1250)));
    follower_stats.push((Value::Text("following_count".to_string()), Value::Integer(890)));
    follower_stats.push((Value::Text("posts_count".to_string()), Value::Integer(456)));
    follower_stats.push((Value::Text("likes_received".to_string()), Value::Integer(15670)));
    
    let stats = Value::Frozen(Box::new(Value::Map(follower_stats)));
    
    let mut profile_row = HashMap::new();
    profile_row.insert("user_id".to_string(), Value::Uuid([30; 16]));
    profile_row.insert("username".to_string(), Value::Text("johndoe".to_string()));
    profile_row.insert("display_name".to_string(), Value::Text("John Doe".to_string()));
    profile_row.insert("bio".to_string(), Value::Text(
        "📸 Travel photographer | 🌍 Exploring the world one city at a time | 💻 Tech enthusiast"
            .to_string()
    ));
    profile_row.insert("social_links".to_string(), Value::List(vec![twitter_link, instagram_link]));
    profile_row.insert("privacy_settings".to_string(), Value::Map(privacy_settings));
    profile_row.insert("interests".to_string(), interests);
    profile_row.insert("recent_activity".to_string(), recent_activity);
    profile_row.insert("location".to_string(), location_data);
    profile_row.insert("follower_stats".to_string(), stats);
    profile_row.insert("avatar_url".to_string(), Value::Text("https://cdn.example.com/avatars/johndoe.jpg".to_string()));
    profile_row.insert("created_at".to_string(), Value::Timestamp(1609459200000000)); // 2021-01-01
    profile_row.insert("last_active".to_string(), Value::Timestamp(1640995200000000));
    
    db.insert("user_profiles", profile_row).await?;
    println!("👤 User profile created successfully");
    Ok(())
}
```

### Post with Rich Media and Engagement

```rust
async fn create_social_media_post() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::new("./social_data").await?;
    
    // Media attachment UDT
    let mut photo_fields = HashMap::new();
    photo_fields.insert("type".to_string(), Value::Text("image".to_string()));
    photo_fields.insert("url".to_string(), Value::Text("https://cdn.example.com/posts/sunset-beach.jpg".to_string()));
    photo_fields.insert("thumbnail_url".to_string(), Value::Text("https://cdn.example.com/thumbnails/sunset-beach-thumb.jpg".to_string()));
    photo_fields.insert("width".to_string(), Value::Integer(1920));
    photo_fields.insert("height".to_string(), Value::Integer(1080));
    photo_fields.insert("file_size".to_string(), Value::Integer(2456789)); // bytes
    photo_fields.insert("alt_text".to_string(), Value::Text("Beautiful sunset over the ocean with waves crashing on the beach".to_string()));
    
    let photo_attachment = Value::Udt("MediaAttachment".to_string(), photo_fields);
    
    // Comments as list of comment UDTs
    let mut comment1_fields = HashMap::new();
    comment1_fields.insert("comment_id".to_string(), Value::Uuid([40; 16]));
    comment1_fields.insert("author_id".to_string(), Value::Uuid([41; 16]));
    comment1_fields.insert("author_username".to_string(), Value::Text("alice_travels".to_string()));
    comment1_fields.insert("text".to_string(), Value::Text("Absolutely stunning! 😍 Where was this taken?".to_string()));
    comment1_fields.insert("created_at".to_string(), Value::Timestamp(1640996400000000));
    comment1_fields.insert("likes_count".to_string(), Value::Integer(15));
    comment1_fields.insert("edited".to_string(), Value::Boolean(false));
    
    let comment1 = Value::Udt("Comment".to_string(), comment1_fields);
    
    let mut comment2_fields = HashMap::new();
    comment2_fields.insert("comment_id".to_string(), Value::Uuid([42; 16]));
    comment2_fields.insert("author_id".to_string(), Value::Uuid([43; 16]));
    comment2_fields.insert("author_username".to_string(), Value::Text("beach_lover_22".to_string()));
    comment2_fields.insert("text".to_string(), Value::Text("This looks like paradise! 🏖️".to_string()));
    comment2_fields.insert("created_at".to_string(), Value::Timestamp(1640997000000000));
    comment2_fields.insert("likes_count".to_string(), Value::Integer(8));
    comment2_fields.insert("edited".to_string(), Value::Boolean(false));
    
    let comment2 = Value::Udt("Comment".to_string(), comment2_fields);
    
    // Hashtags as set
    let hashtags = Value::Set(vec![
        Value::Text("#sunset".to_string()),
        Value::Text("#beach".to_string()),
        Value::Text("#photography".to_string()),
        Value::Text("#nature".to_string()),
        Value::Text("#travel".to_string()),
        Value::Text("#ocean".to_string()),
    ]);
    
    // Mentions as list (preserving order)
    let mentions = Value::List(vec![
        Value::Text("@travel_magazine".to_string()),
        Value::Text("@photography_daily".to_string()),
    ]);
    
    // Engagement metrics as frozen map
    let mut engagement_metrics = Vec::new();
    engagement_metrics.push((Value::Text("likes_count".to_string()), Value::Integer(342)));
    engagement_metrics.push((Value::Text("comments_count".to_string()), Value::Integer(28)));
    engagement_metrics.push((Value::Text("shares_count".to_string()), Value::Integer(15)));
    engagement_metrics.push((Value::Text("saves_count".to_string()), Value::Integer(67)));
    engagement_metrics.push((Value::Text("views_count".to_string()), Value::Integer(1250)));
    
    let engagement = Value::Frozen(Box::new(Value::Map(engagement_metrics)));
    
    // Post metadata
    let mut metadata = Vec::new();
    metadata.push((Value::Text("camera_model".to_string()), Value::Text("Canon EOS R5".to_string())));
    metadata.push((Value::Text("lens".to_string()), Value::Text("RF 24-70mm f/2.8L IS USM".to_string())));
    metadata.push((Value::Text("iso".to_string()), Value::Integer(100)));
    metadata.push((Value::Text("aperture".to_string()), Value::Text("f/8.0".to_string())));
    metadata.push((Value::Text("shutter_speed".to_string()), Value::Text("1/250s".to_string())));
    metadata.push((Value::Text("focal_length".to_string()), Value::Text("35mm".to_string())));
    
    let mut post_row = HashMap::new();
    post_row.insert("post_id".to_string(), Value::Uuid([50; 16]));
    post_row.insert("author_id".to_string(), Value::Uuid([30; 16])); // Same as johndoe profile
    post_row.insert("text".to_string(), Value::Text(
        "Golden hour magic at Malibu Beach 🌅 There's something about the way the light dances on the waves that never gets old. Nature's daily masterpiece! #blessed"
            .to_string()
    ));
    post_row.insert("media_attachments".to_string(), Value::List(vec![photo_attachment]));
    post_row.insert("comments".to_string(), Value::List(vec![comment1, comment2]));
    post_row.insert("hashtags".to_string(), hashtags);
    post_row.insert("mentions".to_string(), mentions);
    post_row.insert("engagement_metrics".to_string(), engagement);
    post_row.insert("metadata".to_string(), Value::Map(metadata));
    post_row.insert("location".to_string(), Value::Text("Malibu, California".to_string()));
    post_row.insert("created_at".to_string(), Value::Timestamp(1640995800000000));
    post_row.insert("updated_at".to_string(), Value::Timestamp(1640995800000000));
    post_row.insert("visibility".to_string(), Value::Text("public".to_string()));
    post_row.insert("allow_comments".to_string(), Value::Boolean(true));
    
    db.insert("posts", post_row).await?;
    println!("📱 Social media post created successfully");
    Ok(())
}
```

## IoT and Analytics Examples

### Sensor Data with Complex Measurements

```rust
async fn store_iot_sensor_data() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::new("./iot_data").await?;
    
    // Environmental readings UDT
    let mut env_readings_fields = HashMap::new();
    env_readings_fields.insert("temperature_celsius".to_string(), Value::Float(23.5));
    env_readings_fields.insert("humidity_percent".to_string(), Value::Float(65.2));
    env_readings_fields.insert("pressure_hpa".to_string(), Value::Float(1013.25));
    env_readings_fields.insert("air_quality_index".to_string(), Value::Integer(42));
    env_readings_fields.insert("uv_index".to_string(), Value::Float(3.2));
    env_readings_fields.insert("light_lux".to_string(), Value::Float(450.0));
    
    let environmental_data = Value::Udt("EnvironmentalReading".to_string(), env_readings_fields);
    
    // Motion sensor data UDT
    let mut motion_fields = HashMap::new();
    motion_fields.insert("acceleration_x".to_string(), Value::Float(0.12));
    motion_fields.insert("acceleration_y".to_string(), Value::Float(-0.05));
    motion_fields.insert("acceleration_z".to_string(), Value::Float(9.81));
    motion_fields.insert("gyroscope_x".to_string(), Value::Float(0.001));
    motion_fields.insert("gyroscope_y".to_string(), Value::Float(-0.002));
    motion_fields.insert("gyroscope_z".to_string(), Value::Float(0.000));
    motion_fields.insert("magnetic_x".to_string(), Value::Float(25.3));
    motion_fields.insert("magnetic_y".to_string(), Value::Float(-12.7));
    motion_fields.insert("magnetic_z".to_string(), Value::Float(48.9));
    
    let motion_data = Value::Udt("MotionReading".to_string(), motion_fields);
    
    // GPS coordinates as tuple
    let gps_location = Value::Tuple(vec![
        Value::Float(37.7749),     // latitude
        Value::Float(-122.4194),   // longitude
        Value::Float(16.5),        // altitude
        Value::Float(2.1),         // accuracy in meters
    ]);
    
    // Battery and power metrics
    let mut power_metrics = Vec::new();
    power_metrics.push((Value::Text("battery_level".to_string()), Value::Float(87.5)));
    power_metrics.push((Value::Text("voltage".to_string()), Value::Float(3.7)));
    power_metrics.push((Value::Text("current_ma".to_string()), Value::Float(45.2)));
    power_metrics.push((Value::Text("power_consumption_mw".to_string()), Value::Float(167.24)));
    power_metrics.push((Value::Text("charging".to_string()), Value::Boolean(false)));
    
    // Network connectivity info
    let mut network_info = Vec::new();
    network_info.push((Value::Text("signal_strength_dbm".to_string()), Value::Integer(-67)));
    network_info.push((Value::Text("network_type".to_string()), Value::Text("4G".to_string())));
    network_info.push((Value::Text("carrier".to_string()), Value::Text("Verizon".to_string())));
    network_info.push((Value::Text("data_usage_mb".to_string()), Value::Float(12.3)));
    
    // Alert conditions as set
    let alert_conditions = Value::Set(vec![
        Value::Text("high_temperature".to_string()),
        Value::Text("low_battery".to_string()),
    ]);
    
    // Historical readings as list (last 10 readings)
    let historical_temps = Value::List(vec![
        Value::Float(22.8), Value::Float(23.1), Value::Float(23.3),
        Value::Float(23.4), Value::Float(23.6), Value::Float(23.5),
        Value::Float(23.7), Value::Float(23.5), Value::Float(23.6),
        Value::Float(23.5), // current reading
    ]);
    
    // Device calibration data as frozen tuple (immutable)
    let calibration_data = Value::Frozen(Box::new(Value::Tuple(vec![
        Value::Timestamp(1640995200000000), // last calibration date
        Value::Float(0.98),                 // temperature offset
        Value::Float(1.02),                 // humidity multiplier
        Value::Float(-2.1),                 // pressure offset
        Value::Boolean(true),               // calibration valid
    ])));
    
    let mut sensor_row = HashMap::new();
    sensor_row.insert("device_id".to_string(), Value::Text("ENV-SENSOR-001".to_string()));
    sensor_row.insert("timestamp".to_string(), Value::Timestamp(1640995800000000));
    sensor_row.insert("environmental_data".to_string(), environmental_data);
    sensor_row.insert("motion_data".to_string(), motion_data);
    sensor_row.insert("gps_location".to_string(), gps_location);
    sensor_row.insert("power_metrics".to_string(), Value::Map(power_metrics));
    sensor_row.insert("network_info".to_string(), Value::Map(network_info));
    sensor_row.insert("alert_conditions".to_string(), alert_conditions);
    sensor_row.insert("historical_temperatures".to_string(), historical_temps);
    sensor_row.insert("calibration_data".to_string(), calibration_data);
    sensor_row.insert("firmware_version".to_string(), Value::Text("v2.1.3".to_string()));
    sensor_row.insert("uptime_seconds".to_string(), Value::Integer(2847293));
    
    db.insert("sensor_readings", sensor_row).await?;
    println!("🌡️ IoT sensor data stored successfully");
    Ok(())
}
```

### Analytics Event Tracking

```rust
async fn track_analytics_event() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::new("./analytics_data").await?;
    
    // User session info UDT
    let mut session_fields = HashMap::new();
    session_fields.insert("session_id".to_string(), Value::Text("sess_abc123xyz789".to_string()));
    session_fields.insert("user_agent".to_string(), Value::Text(
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36"
            .to_string()
    ));
    session_fields.insert("ip_address".to_string(), Value::Text("192.168.1.100".to_string()));
    session_fields.insert("device_type".to_string(), Value::Text("desktop".to_string()));
    session_fields.insert("browser".to_string(), Value::Text("Chrome".to_string()));
    session_fields.insert("os".to_string(), Value::Text("Windows 10".to_string()));
    session_fields.insert("screen_resolution".to_string(), Value::Text("1920x1080".to_string()));
    session_fields.insert("timezone".to_string(), Value::Text("America/New_York".to_string()));
    
    let session_info = Value::Udt("SessionInfo".to_string(), session_fields);
    
    // Page view event UDT
    let mut page_event_fields = HashMap::new();
    page_event_fields.insert("page_url".to_string(), Value::Text("https://example.com/products/laptop-pro".to_string()));
    page_event_fields.insert("page_title".to_string(), Value::Text("Laptop Pro - High Performance Computing".to_string()));
    page_event_fields.insert("referrer".to_string(), Value::Text("https://google.com/search".to_string()));
    page_event_fields.insert("load_time_ms".to_string(), Value::Integer(1250)));
    page_event_fields.insert("scroll_depth_percent".to_string(), Value::Float(75.5));
    page_event_fields.insert("time_on_page_seconds".to_string(), Value::Integer(180));
    
    let page_event = Value::Udt("PageViewEvent".to_string(), page_event_fields);
    
    // UTM parameters as map
    let mut utm_params = Vec::new();
    utm_params.push((Value::Text("utm_source".to_string()), Value::Text("google".to_string())));
    utm_params.push((Value::Text("utm_medium".to_string()), Value::Text("cpc".to_string())));
    utm_params.push((Value::Text("utm_campaign".to_string()), Value::Text("holiday_sale_2022".to_string())));
    utm_params.push((Value::Text("utm_term".to_string()), Value::Text("gaming laptop".to_string())));
    utm_params.push((Value::Text("utm_content".to_string()), Value::Text("ad_variant_a".to_string())));
    
    // Custom dimensions as map
    let mut custom_dimensions = Vec::new();
    custom_dimensions.push((Value::Text("user_segment".to_string()), Value::Text("premium".to_string())));
    custom_dimensions.push((Value::Text("ab_test_group".to_string()), Value::Text("variant_b".to_string())));
    custom_dimensions.push((Value::Text("loyalty_tier".to_string()), Value::Text("gold".to_string())));
    custom_dimensions.push((Value::Text("product_category".to_string()), Value::Text("electronics".to_string())));
    
    // Event properties as nested map
    let mut event_properties = Vec::new();
    event_properties.push((Value::Text("product_id".to_string()), Value::Text("LAPTOP-PRO-15".to_string())));
    event_properties.push((Value::Text("product_price".to_string()), Value::Float(1999.99)));
    event_properties.push((Value::Text("currency".to_string()), Value::Text("USD".to_string())));
    event_properties.push((Value::Text("in_stock".to_string()), Value::Boolean(true)));
    event_properties.push((Value::Text("review_count".to_string()), Value::Integer(248)));
    event_properties.push((Value::Text("avg_rating".to_string()), Value::Float(4.7)));
    
    // Conversion funnel step as list
    let funnel_steps = Value::List(vec![
        Value::Text("homepage_visit".to_string()),
        Value::Text("category_browse".to_string()),
        Value::Text("product_view".to_string()), // Current step
        Value::Text("add_to_cart".to_string()),  // Next steps
        Value::Text("checkout".to_string()),
        Value::Text("purchase".to_string()),
    ]);
    
    // Geographic data as tuple
    let geographic_data = Value::Tuple(vec![
        Value::Text("United States".to_string()),  // country
        Value::Text("New York".to_string()),       // state/region
        Value::Text("New York".to_string()),       // city
        Value::Text("10001".to_string()),         // postal code
        Value::Float(40.7589),                    // latitude
        Value::Float(-73.9851),                   // longitude
    ]);
    
    // A/B test data as frozen map (immutable for analysis)
    let mut ab_test_data = Vec::new();
    ab_test_data.push((Value::Text("test_name".to_string()), Value::Text("product_page_layout".to_string())));
    ab_test_data.push((Value::Text("variant".to_string()), Value::Text("B".to_string())));
    ab_test_data.push((Value::Text("test_start_date".to_string()), Value::Timestamp(1640908800000000)));
    ab_test_data.push((Value::Text("user_in_test".to_string()), Value::Boolean(true)));
    
    let ab_test_info = Value::Frozen(Box::new(Value::Map(ab_test_data)));
    
    let mut event_row = HashMap::new();
    event_row.insert("event_id".to_string(), Value::Uuid([60; 16]));
    event_row.insert("user_id".to_string(), Value::Uuid([61; 16]));
    event_row.insert("anonymous_id".to_string(), Value::Text("anon_987654321".to_string()));
    event_row.insert("event_type".to_string(), Value::Text("page_view".to_string()));
    event_row.insert("timestamp".to_string(), Value::Timestamp(1640995800000000));
    event_row.insert("session_info".to_string(), session_info);
    event_row.insert("page_event".to_string(), page_event);
    event_row.insert("utm_parameters".to_string(), Value::Map(utm_params));
    event_row.insert("custom_dimensions".to_string(), Value::Map(custom_dimensions));
    event_row.insert("event_properties".to_string(), Value::Map(event_properties));
    event_row.insert("funnel_steps".to_string(), funnel_steps);
    event_row.insert("geographic_data".to_string(), geographic_data);
    event_row.insert("ab_test_info".to_string(), ab_test_info);
    event_row.insert("processed".to_string(), Value::Boolean(false));
    
    db.insert("analytics_events", event_row).await?;
    println!("📊 Analytics event tracked successfully");
    Ok(())
}
```

## Financial Services Examples

### Transaction with Complex Metadata

```rust
async fn record_financial_transaction() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::new("./financial_data").await?;
    
    // Account info UDT
    let mut from_account_fields = HashMap::new();
    from_account_fields.insert("account_id".to_string(), Value::Text("ACC-123-456-789".to_string()));
    from_account_fields.insert("account_type".to_string(), Value::Text("checking".to_string()));
    from_account_fields.insert("bank_code".to_string(), Value::Text("CHASE".to_string()));
    from_account_fields.insert("routing_number".to_string(), Value::Text("021000021".to_string()));
    from_account_fields.insert("currency".to_string(), Value::Text("USD".to_string()));
    
    let from_account = Value::Udt("AccountInfo".to_string(), from_account_fields);
    
    let mut to_account_fields = HashMap::new();
    to_account_fields.insert("account_id".to_string(), Value::Text("ACC-987-654-321".to_string()));
    to_account_fields.insert("account_type".to_string(), Value::Text("savings".to_string()));
    to_account_fields.insert("bank_code".to_string(), Value::Text("WELLS".to_string()));
    to_account_fields.insert("routing_number".to_string(), Value::Text("121000248".to_string()));
    to_account_fields.insert("currency".to_string(), Value::Text("USD".to_string()));
    
    let to_account = Value::Udt("AccountInfo".to_string(), to_account_fields);
    
    // Transaction amounts with fees breakdown
    let mut amount_breakdown = Vec::new();
    amount_breakdown.push((Value::Text("principal".to_string()), Value::Float(1000.00)));
    amount_breakdown.push((Value::Text("processing_fee".to_string()), Value::Float(2.50)));
    amount_breakdown.push((Value::Text("network_fee".to_string()), Value::Float(0.30)));
    amount_breakdown.push((Value::Text("total".to_string()), Value::Float(1002.80)));
    
    // Risk assessment data
    let mut risk_factors = Vec::new();
    risk_factors.push((Value::Text("fraud_score".to_string()), Value::Float(0.15))); // Low risk
    risk_factors.push((Value::Text("velocity_check".to_string()), Value::Boolean(true)));
    risk_factors.push((Value::Text("location_match".to_string()), Value::Boolean(true)));
    risk_factors.push((Value::Text("device_recognized".to_string()), Value::Boolean(true)));
    risk_factors.push((Value::Text("pattern_analysis".to_string()), Value::Text("normal".to_string())));
    
    // Compliance data as frozen (immutable for audit)
    let mut compliance_data = Vec::new();
    compliance_data.push((Value::Text("aml_checked".to_string()), Value::Boolean(true)));
    compliance_data.push((Value::Text("sanctions_checked".to_string()), Value::Boolean(true)));
    compliance_data.push((Value::Text("kyc_verified".to_string()), Value::Boolean(true)));
    compliance_data.push((Value::Text("pep_screening".to_string()), Value::Boolean(true)));
    compliance_data.push((Value::Text("compliance_officer_id".to_string()), Value::Text("CO-789".to_string())));
    compliance_data.push((Value::Text("review_required".to_string()), Value::Boolean(false)));
    
    let compliance_info = Value::Frozen(Box::new(Value::Map(compliance_data)));
    
    // Transaction flags as set
    let transaction_flags = Value::Set(vec![
        Value::Text("domestic".to_string()),
        Value::Text("same_day".to_string()),
        Value::Text("verified".to_string()),
        Value::Text("automated".to_string()),
    ]);
    
    // Processing timeline as list
    let processing_timeline = Value::List(vec![
        Value::Text("submitted".to_string()),
        Value::Text("validated".to_string()),
        Value::Text("risk_assessed".to_string()),
        Value::Text("compliance_checked".to_string()),
        Value::Text("approved".to_string()),
        Value::Text("processing".to_string()),
    ]);
    
    // Exchange rate data for international transactions
    let exchange_rate_info = Value::Tuple(vec![
        Value::Text("USD".to_string()),    // from currency
        Value::Text("USD".to_string()),    // to currency  
        Value::Float(1.0),                 // exchange rate
        Value::Timestamp(1640995800000000), // rate timestamp
        Value::Text("XE.com".to_string()), // rate source
    ]);
    
    let mut transaction_row = HashMap::new();
    transaction_row.insert("transaction_id".to_string(), Value::Uuid([70; 16]));
    transaction_row.insert("from_account".to_string(), from_account);
    transaction_row.insert("to_account".to_string(), to_account);
    transaction_row.insert("amount_breakdown".to_string(), Value::Map(amount_breakdown));
    transaction_row.insert("transaction_type".to_string(), Value::Text("transfer".to_string()));
    transaction_row.insert("description".to_string(), Value::Text("Monthly savings transfer".to_string()));
    transaction_row.insert("risk_assessment".to_string(), Value::Map(risk_factors));
    transaction_row.insert("compliance_info".to_string(), compliance_info);
    transaction_row.insert("transaction_flags".to_string(), transaction_flags);
    transaction_row.insert("processing_timeline".to_string(), processing_timeline);
    transaction_row.insert("exchange_rate_info".to_string(), exchange_rate_info);
    transaction_row.insert("reference_number".to_string(), Value::Text("TXN-2022-001-ABC123".to_string()));
    transaction_row.insert("initiated_at".to_string(), Value::Timestamp(1640995800000000));
    transaction_row.insert("completed_at".to_string(), Value::Timestamp(1640996100000000));
    transaction_row.insert("status".to_string(), Value::Text("completed".to_string()));
    
    db.insert("transactions", transaction_row).await?;
    println!("💰 Financial transaction recorded successfully");
    Ok(())
}
```

## Healthcare Data Examples

### Patient Record with Medical History

```rust
async fn create_patient_record() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::new("./healthcare_data").await?;
    
    // Medical condition UDT
    let mut condition1_fields = HashMap::new();
    condition1_fields.insert("condition_code".to_string(), Value::Text("E11.9".to_string())); // ICD-10
    condition1_fields.insert("condition_name".to_string(), Value::Text("Type 2 diabetes mellitus without complications".to_string()));
    condition1_fields.insert("severity".to_string(), Value::Text("moderate".to_string()));
    condition1_fields.insert("diagnosed_date".to_string(), Value::Timestamp(1577836800000000)); // 2020-01-01
    condition1_fields.insert("status".to_string(), Value::Text("active".to_string()));
    condition1_fields.insert("notes".to_string(), Value::Text("Well controlled with medication".to_string()));
    
    let diabetes_condition = Value::Udt("MedicalCondition".to_string(), condition1_fields);
    
    let mut condition2_fields = HashMap::new();
    condition2_fields.insert("condition_code".to_string(), Value::Text("I10".to_string()));
    condition2_fields.insert("condition_name".to_string(), Value::Text("Essential hypertension".to_string()));
    condition2_fields.insert("severity".to_string(), Value::Text("mild".to_string()));
    condition2_fields.insert("diagnosed_date".to_string(), Value::Timestamp(1609459200000000)); // 2021-01-01
    condition2_fields.insert("status".to_string(), Value::Text("active".to_string()));
    condition2_fields.insert("notes".to_string(), Value::Text("Responsive to ACE inhibitor therapy".to_string()));
    
    let hypertension_condition = Value::Udt("MedicalCondition".to_string(), condition2_fields);
    
    // Medication UDT
    let mut med1_fields = HashMap::new();
    med1_fields.insert("medication_name".to_string(), Value::Text("Metformin".to_string()));
    med1_fields.insert("dosage".to_string(), Value::Text("500mg".to_string()));
    med1_fields.insert("frequency".to_string(), Value::Text("twice daily".to_string()));
    med1_fields.insert("route".to_string(), Value::Text("oral".to_string()));
    med1_fields.insert("prescribing_doctor".to_string(), Value::Text("Dr. Sarah Johnson".to_string()));
    med1_fields.insert("start_date".to_string(), Value::Timestamp(1577836800000000));
    med1_fields.insert("active".to_string(), Value::Boolean(true));
    
    let metformin = Value::Udt("Medication".to_string(), med1_fields);
    
    // Vital signs as time-series list
    let mut vital_signs_day1 = HashMap::new();
    vital_signs_day1.insert("timestamp".to_string(), Value::Timestamp(1640995200000000));
    vital_signs_day1.insert("blood_pressure_systolic".to_string(), Value::Integer(128));
    vital_signs_day1.insert("blood_pressure_diastolic".to_string(), Value::Integer(82));
    vital_signs_day1.insert("heart_rate".to_string(), Value::Integer(72));
    vital_signs_day1.insert("temperature_celsius".to_string(), Value::Float(36.8));
    vital_signs_day1.insert("oxygen_saturation".to_string(), Value::Float(98.5));
    vital_signs_day1.insert("weight_kg".to_string(), Value::Float(75.2));
    
    let vitals1 = Value::Udt("VitalSigns".to_string(), vital_signs_day1);
    
    let mut vital_signs_day2 = HashMap::new();
    vital_signs_day2.insert("timestamp".to_string(), Value::Timestamp(1641081600000000));
    vital_signs_day2.insert("blood_pressure_systolic".to_string(), Value::Integer(125));
    vital_signs_day2.insert("blood_pressure_diastolic".to_string(), Value::Integer(79));
    vital_signs_day2.insert("heart_rate".to_string(), Value::Integer(68));
    vital_signs_day2.insert("temperature_celsius".to_string(), Value::Float(36.9));
    vital_signs_day2.insert("oxygen_saturation".to_string(), Value::Float(99.0)));
    vital_signs_day2.insert("weight_kg".to_string(), Value::Float(75.0));
    
    let vitals2 = Value::Udt("VitalSigns".to_string(), vital_signs_day2);
    
    // Allergies as set
    let allergies = Value::Set(vec![
        Value::Text("penicillin".to_string()),
        Value::Text("shellfish".to_string()),
        Value::Text("latex".to_string()),
    ]);
    
    // Lab results as map with test codes and values
    let mut lab_results = Vec::new();
    lab_results.push((Value::Text("HbA1c".to_string()), Value::Float(7.2))); // %
    lab_results.push((Value::Text("glucose_fasting".to_string()), Value::Float(126.0))); // mg/dL
    lab_results.push((Value::Text("cholesterol_total".to_string()), Value::Float(195.0))); // mg/dL
    lab_results.push((Value::Text("hdl_cholesterol".to_string()), Value::Float(45.0))); // mg/dL
    lab_results.push((Value::Text("ldl_cholesterol".to_string()), Value::Float(125.0))); // mg/dL
    lab_results.push((Value::Text("triglycerides".to_string()), Value::Float(150.0))); // mg/dL
    
    // Emergency contacts as list
    let mut emergency_contact1 = HashMap::new();
    emergency_contact1.insert("name".to_string(), Value::Text("Jane Doe".to_string()));
    emergency_contact1.insert("relationship".to_string(), Value::Text("spouse".to_string()));
    emergency_contact1.insert("phone_primary".to_string(), Value::Text("+1-555-0123".to_string()));
    emergency_contact1.insert("phone_secondary".to_string(), Value::Text("+1-555-0124".to_string()));
    emergency_contact1.insert("email".to_string(), Value::Text("jane.doe@email.com".to_string()));
    
    let contact1 = Value::Udt("EmergencyContact".to_string(), emergency_contact1);
    
    // Insurance information as frozen (immutable for billing)
    let mut insurance_data = Vec::new();
    insurance_data.push((Value::Text("provider".to_string()), Value::Text("Blue Cross Blue Shield".to_string())));
    insurance_data.push((Value::Text("policy_number".to_string()), Value::Text("BCBS-123456789".to_string())));
    insurance_data.push((Value::Text("group_number".to_string()), Value::Text("GRP-987654".to_string())));
    insurance_data.push((Value::Text("copay_primary".to_string()), Value::Float(25.00)));
    insurance_data.push((Value::Text("copay_specialist".to_string()), Value::Float(50.00)));
    insurance_data.push((Value::Text("deductible_annual".to_string()), Value::Float(2000.00)));
    insurance_data.push((Value::Text("out_of_pocket_max".to_string()), Value::Float(8000.00)));
    
    let insurance_info = Value::Frozen(Box::new(Value::Map(insurance_data)));
    
    let mut patient_row = HashMap::new();
    patient_row.insert("patient_id".to_string(), Value::Uuid([80; 16]));
    patient_row.insert("medical_record_number".to_string(), Value::Text("MRN-2022-001234".to_string()));
    patient_row.insert("first_name".to_string(), Value::Text("John".to_string()));
    patient_row.insert("last_name".to_string(), Value::Text("Doe".to_string()));
    patient_row.insert("date_of_birth".to_string(), Value::Timestamp(315532800000000)); // 1980-01-01
    patient_row.insert("medical_conditions".to_string(), Value::List(vec![diabetes_condition, hypertension_condition]));
    patient_row.insert("current_medications".to_string(), Value::List(vec![metformin]));
    patient_row.insert("vital_signs_history".to_string(), Value::List(vec![vitals1, vitals2]));
    patient_row.insert("allergies".to_string(), allergies);
    patient_row.insert("latest_lab_results".to_string(), Value::Map(lab_results));
    patient_row.insert("emergency_contacts".to_string(), Value::List(vec![contact1]));
    patient_row.insert("insurance_info".to_string(), insurance_info);
    patient_row.insert("primary_physician".to_string(), Value::Text("Dr. Sarah Johnson".to_string()));
    patient_row.insert("created_at".to_string(), Value::Timestamp(1640995200000000));
    patient_row.insert("last_updated".to_string(), Value::Timestamp(1641081600000000));
    
    db.insert("patient_records", patient_row).await?;
    println!("🏥 Patient record created successfully");
    Ok(())
}
```

## Performance Monitoring Examples

### Application Performance Data

```rust
async fn record_application_metrics() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::new("./monitoring_data").await?;
    
    // Service performance metrics UDT
    let mut service_metrics = HashMap::new();
    service_metrics.insert("response_time_p50".to_string(), Value::Float(125.5));  // ms
    service_metrics.insert("response_time_p95".to_string(), Value::Float(450.2));  // ms
    service_metrics.insert("response_time_p99".to_string(), Value::Float(890.1));  // ms
    service_metrics.insert("requests_per_second".to_string(), Value::Float(342.7));
    service_metrics.insert("error_rate_percent".to_string(), Value::Float(0.15));
    service_metrics.insert("cpu_usage_percent".to_string(), Value::Float(65.3));
    service_metrics.insert("memory_usage_mb".to_string(), Value::Float(1250.8));
    service_metrics.insert("active_connections".to_string(), Value::Integer(145));
    
    let performance_metrics = Value::Udt("ServiceMetrics".to_string(), service_metrics);
    
    // Database performance UDT
    let mut db_metrics = HashMap::new();
    db_metrics.insert("query_time_avg_ms".to_string(), Value::Float(45.2));
    db_metrics.insert("query_time_max_ms".to_string(), Value::Float(2300.0));
    db_metrics.insert("queries_per_second".to_string(), Value::Float(85.4));
    db_metrics.insert("slow_queries_count".to_string(), Value::Integer(3));
    db_metrics.insert("connection_pool_active".to_string(), Value::Integer(12));
    db_metrics.insert("connection_pool_idle".to_string(), Value::Integer(8));
    db_metrics.insert("lock_waits_count".to_string(), Value::Integer(0));
    db_metrics.insert("deadlocks_count".to_string(), Value::Integer(0));
    
    let database_metrics = Value::Udt("DatabaseMetrics".to_string(), db_metrics);
    
    // Error tracking as list of error types
    let error_breakdown = Value::List(vec![
        Value::Text("timeout_errors: 2".to_string()),
        Value::Text("validation_errors: 15".to_string()),
        Value::Text("authentication_errors: 8".to_string()),
        Value::Text("rate_limit_errors: 3".to_string()),
        Value::Text("internal_server_errors: 1".to_string()),
    ]);
    
    // Resource utilization as map
    let mut resource_usage = Vec::new();
    resource_usage.push((Value::Text("disk_usage_gb".to_string()), Value::Float(250.7)));
    resource_usage.push((Value::Text("disk_available_gb".to_string()), Value::Float(749.3)));
    resource_usage.push((Value::Text("network_in_mbps".to_string()), Value::Float(45.6)));
    resource_usage.push((Value::Text("network_out_mbps".to_string()), Value::Float(78.9)));
    resource_usage.push((Value::Text("load_average_1m".to_string()), Value::Float(2.34)));
    resource_usage.push((Value::Text("load_average_5m".to_string()), Value::Float(2.12)));
    resource_usage.push((Value::Text("load_average_15m".to_string()), Value::Float(1.98)));
    
    // Alert conditions as set
    let active_alerts = Value::Set(vec![
        Value::Text("high_response_time".to_string()),
        Value::Text("memory_usage_warning".to_string()),
    ]);
    
    // SLA compliance data as frozen (immutable for reporting)
    let mut sla_data = Vec::new();
    sla_data.push((Value::Text("uptime_percent".to_string()), Value::Float(99.95)));
    sla_data.push((Value::Text("availability_target".to_string()), Value::Float(99.9)));
    sla_data.push((Value::Text("response_time_target_ms".to_string()), Value::Float(200.0)));
    sla_data.push((Value::Text("error_rate_target_percent".to_string()), Value::Float(0.1)));
    sla_data.push((Value::Text("compliance_status".to_string()), Value::Text("meeting".to_string())));
    
    let sla_compliance = Value::Frozen(Box::new(Value::Map(sla_data)));
    
    // Historical data points as list (last 24 hours, hourly)
    let cpu_usage_24h = Value::List(vec![
        Value::Float(58.2), Value::Float(62.1), Value::Float(59.8), Value::Float(61.4),
        Value::Float(65.7), Value::Float(68.9), Value::Float(71.2), Value::Float(69.5),
        Value::Float(66.8), Value::Float(63.4), Value::Float(60.2), Value::Float(58.9),
        Value::Float(55.1), Value::Float(52.7), Value::Float(54.3), Value::Float(57.8),
        Value::Float(61.2), Value::Float(64.5), Value::Float(67.3), Value::Float(69.8),
        Value::Float(68.1), Value::Float(66.4), Value::Float(64.7), Value::Float(65.3),
    ]);
    
    // Service dependencies health
    let mut dependency_health = Vec::new();
    dependency_health.push((Value::Text("redis_cache".to_string()), Value::Text("healthy".to_string())));
    dependency_health.push((Value::Text("postgres_db".to_string()), Value::Text("healthy".to_string())));
    dependency_health.push((Value::Text("elasticsearch".to_string()), Value::Text("degraded".to_string())));
    dependency_health.push((Value::Text("payment_api".to_string()), Value::Text("healthy".to_string())));
    dependency_health.push((Value::Text("email_service".to_string()), Value::Text("healthy".to_string())));
    
    let mut metrics_row = HashMap::new();
    metrics_row.insert("metric_id".to_string(), Value::Uuid([90; 16]));
    metrics_row.insert("service_name".to_string(), Value::Text("web-api".to_string()));
    metrics_row.insert("environment".to_string(), Value::Text("production".to_string()));
    metrics_row.insert("timestamp".to_string(), Value::Timestamp(1640995800000000));
    metrics_row.insert("service_metrics".to_string(), performance_metrics);
    metrics_row.insert("database_metrics".to_string(), database_metrics);
    metrics_row.insert("error_breakdown".to_string(), error_breakdown);
    metrics_row.insert("resource_usage".to_string(), Value::Map(resource_usage));
    metrics_row.insert("active_alerts".to_string(), active_alerts);
    metrics_row.insert("sla_compliance".to_string(), sla_compliance);
    metrics_row.insert("cpu_usage_24h".to_string(), cpu_usage_24h);
    metrics_row.insert("dependency_health".to_string(), Value::Map(dependency_health));
    metrics_row.insert("version".to_string(), Value::Text("v2.1.4".to_string()));
    metrics_row.insert("instance_id".to_string(), Value::Text("web-api-prod-01".to_string()));
    
    db.insert("application_metrics", metrics_row).await?;
    println!("📈 Application metrics recorded successfully");
    Ok(())
}
```

## Best Practices Summary

### 1. Data Structure Design

```rust
// ✅ Good: Logical grouping of related fields
let mut user_profile = HashMap::new();
user_profile.insert("basic_info".to_string(), basic_info_udt);
user_profile.insert("preferences".to_string(), preferences_map);
user_profile.insert("activity_history".to_string(), activity_list);

// ❌ Avoid: Flat structure for complex data
let mut flat_user = HashMap::new();
flat_user.insert("name".to_string(), Value::Text("John".to_string()));
flat_user.insert("email".to_string(), Value::Text("john@example.com".to_string()));
flat_user.insert("pref_theme".to_string(), Value::Text("dark".to_string()));
flat_user.insert("pref_language".to_string(), Value::Text("en".to_string()));
// ... many more individual fields
```

### 2. Performance Considerations

```rust
// ✅ Good: Use frozen for immutable lookup data
let config_data = Value::Frozen(Box::new(Value::Map(configuration)));

// ✅ Good: Use appropriate collection size
let recent_items = Value::List(items.into_iter().take(100).collect()); // Limit size

// ❌ Avoid: Unbounded collections
let all_items = Value::List(millions_of_items); // Could cause memory issues
```

### 3. Type Safety

```rust
// ✅ Good: Validate data before storage
fn validate_email_address(email: &str) -> bool {
    email.contains('@') && email.contains('.')
}

// Use validation before creating UDT
if validate_email_address(&email) {
    user_fields.insert("email".to_string(), Value::Text(email));
}
```

These examples demonstrate real-world usage patterns that you can adapt for your specific use cases. Each example shows proper structure, validation, and performance considerations for production systems.