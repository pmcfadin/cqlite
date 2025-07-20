#!/usr/bin/env python3
"""
Generate realistic production-scale datasets for CQLite testing.

This script creates test data that mimics real-world Cassandra usage patterns:
- IoT sensor data with time series patterns
- User profile data with complex nested structures
- Content management data with full-text search patterns
- Analytics data with aggregation-friendly structures
"""

import json
import random
import uuid
import time
from datetime import datetime, timedelta
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import argparse
import sys

class ProductionDatasetGenerator:
    def __init__(self, contact_points=None, keyspace='cqlite_production_test'):
        self.contact_points = contact_points or ['localhost']
        self.keyspace = keyspace
        self.cluster = None
        self.session = None
        
    def connect(self):
        """Connect to Cassandra cluster"""
        try:
            self.cluster = Cluster(self.contact_points)
            self.session = self.cluster.connect()
            print(f"✅ Connected to Cassandra cluster: {self.contact_points}")
        except Exception as e:
            print(f"❌ Failed to connect to Cassandra: {e}")
            sys.exit(1)
    
    def create_schemas(self):
        """Create production-like schemas"""
        print("📝 Creating production-like schemas...")
        
        # Create keyspace
        self.session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {self.keyspace}
            WITH REPLICATION = {{
                'class': 'SimpleStrategy',
                'replication_factor': 3
            }}
        """)
        
        self.session.execute(f"USE {self.keyspace}")
        
        # 1. IoT Sensor Data Table (Time Series Pattern)
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS iot_sensor_data (
                device_id TEXT,
                sensor_type TEXT,
                year INT,
                month INT,
                day INT,
                reading_time TIMESTAMP,
                value DOUBLE,
                unit TEXT,
                quality_score FLOAT,
                location_lat DOUBLE,
                location_lng DOUBLE,
                metadata MAP<TEXT, TEXT>,
                tags SET<TEXT>,
                PRIMARY KEY ((device_id, sensor_type, year, month), day, reading_time)
            ) WITH CLUSTERING ORDER BY (day DESC, reading_time DESC)
        """)
        
        # 2. User Profile Data (Complex Nested Structures)
        self.session.execute("""
            CREATE TYPE IF NOT EXISTS address_type (
                street TEXT,
                city TEXT,
                state TEXT,
                zip_code TEXT,
                country TEXT,
                coordinates FROZEN<MAP<TEXT, DOUBLE>>
            )
        """)
        
        self.session.execute("""
            CREATE TYPE IF NOT EXISTS social_profile (
                platform TEXT,
                username TEXT,
                verified BOOLEAN,
                followers_count INT,
                metadata MAP<TEXT, TEXT>
            )
        """)
        
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS user_profiles (
                user_id UUID,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                email TEXT,
                username TEXT,
                full_name TEXT,
                birth_date DATE,
                profile_data MAP<TEXT, TEXT>,
                addresses LIST<FROZEN<address_type>>,
                social_profiles LIST<FROZEN<social_profile>>,
                preferences MAP<TEXT, TEXT>,
                tags SET<TEXT>,
                activity_score DOUBLE,
                last_login TIMESTAMP,
                metadata MAP<TEXT, TEXT>,
                PRIMARY KEY (user_id)
            )
        """)
        
        # 3. Content Management System (Full-text and Complex Queries)
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS content_items (
                content_id UUID,
                category TEXT,
                subcategory TEXT,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                author_id UUID,
                title TEXT,
                content_text TEXT,
                summary TEXT,
                keywords SET<TEXT>,
                tags MAP<TEXT, TEXT>,
                view_count BIGINT,
                like_count INT,
                comment_count INT,
                metadata MAP<TEXT, TEXT>,
                attachments LIST<TEXT>,
                status TEXT,
                published_at TIMESTAMP,
                PRIMARY KEY ((category, subcategory), created_at, content_id)
            ) WITH CLUSTERING ORDER BY (created_at DESC, content_id ASC)
        """)
        
        # 4. Analytics Data (Aggregation-friendly)
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS analytics_events (
                event_type TEXT,
                year INT,
                month INT,
                day INT,
                hour INT,
                event_time TIMESTAMP,
                event_id TIMEUUID,
                user_id UUID,
                session_id TEXT,
                properties MAP<TEXT, TEXT>,
                numeric_properties MAP<TEXT, DOUBLE>,
                event_data BLOB,
                ip_address INET,
                user_agent TEXT,
                referrer TEXT,
                page_url TEXT,
                PRIMARY KEY ((event_type, year, month, day), hour, event_time, event_id)
            ) WITH CLUSTERING ORDER BY (hour DESC, event_time DESC, event_id DESC)
        """)
        
        # 5. Large Binary Data Table
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS binary_assets (
                asset_id UUID,
                asset_type TEXT,
                size_category TEXT,
                created_at TIMESTAMP,
                filename TEXT,
                mime_type TEXT,
                file_size BIGINT,
                checksum TEXT,
                binary_data BLOB,
                thumbnail_data BLOB,
                metadata MAP<TEXT, TEXT>,
                tags SET<TEXT>,
                PRIMARY KEY ((asset_type, size_category), created_at, asset_id)
            ) WITH CLUSTERING ORDER BY (created_at DESC, asset_id ASC)
        """)
        
        # Create secondary indexes for complex queries
        self.session.execute("CREATE INDEX IF NOT EXISTS idx_user_email ON user_profiles (email)")
        self.session.execute("CREATE INDEX IF NOT EXISTS idx_content_author ON content_items (author_id)")
        self.session.execute("CREATE INDEX IF NOT EXISTS idx_content_status ON content_items (status)")
        
        print("✅ Production schemas created successfully")
    
    def generate_iot_data(self, device_count=1000, days=30, readings_per_day=144):
        """Generate IoT sensor data (every 10 minutes for specified days)"""
        print(f"📊 Generating IoT data: {device_count} devices × {days} days × {readings_per_day} readings...")
        
        sensor_types = ['temperature', 'humidity', 'pressure', 'light', 'motion', 'air_quality']
        units = {'temperature': 'celsius', 'humidity': 'percent', 'pressure': 'hpa', 
                'light': 'lux', 'motion': 'boolean', 'air_quality': 'ppm'}
        
        locations = [
            (40.7128, -74.0060),  # New York
            (34.0522, -118.2437), # Los Angeles  
            (41.8781, -87.6298),  # Chicago
            (29.7604, -95.3698),  # Houston
            (39.9526, -75.1652),  # Philadelphia
        ]
        
        base_date = datetime.now() - timedelta(days=days)
        batch_size = 1000
        total_records = 0
        
        prepared_stmt = self.session.prepare("""
            INSERT INTO iot_sensor_data (
                device_id, sensor_type, year, month, day, reading_time,
                value, unit, quality_score, location_lat, location_lng,
                metadata, tags
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)
        
        for device_num in range(device_count):
            device_id = f"device_{device_num:06d}"
            sensor_type = random.choice(sensor_types)
            unit = units[sensor_type]
            location_lat, location_lng = random.choice(locations)
            # Add some random variance to location
            location_lat += random.uniform(-0.1, 0.1)
            location_lng += random.uniform(-0.1, 0.1)
            
            batch_data = []
            
            for day_offset in range(days):
                current_date = base_date + timedelta(days=day_offset)
                year, month, day = current_date.year, current_date.month, current_date.day
                
                for reading_num in range(readings_per_day):
                    reading_time = current_date + timedelta(minutes=reading_num * 10)
                    
                    # Generate realistic sensor values
                    if sensor_type == 'temperature':
                        base_temp = 20 + 10 * math.sin(2 * math.pi * day_offset / 365)  # Seasonal variation
                        daily_variation = 5 * math.sin(2 * math.pi * reading_num / readings_per_day)  # Daily variation
                        value = base_temp + daily_variation + random.uniform(-2, 2)
                    elif sensor_type == 'humidity':
                        value = 50 + 20 * random.random() + 10 * math.sin(2 * math.pi * reading_num / readings_per_day)
                    elif sensor_type == 'pressure':
                        value = 1013.25 + random.uniform(-20, 20)
                    elif sensor_type == 'light':
                        if 6 <= reading_time.hour <= 18:  # Daylight hours
                            value = 200 + 800 * random.random()
                        else:
                            value = 5 + 50 * random.random()
                    elif sensor_type == 'motion':
                        value = 1 if random.random() < 0.1 else 0  # 10% motion detection
                    else:  # air_quality
                        value = 10 + 50 * random.random()
                    
                    quality_score = 0.8 + 0.2 * random.random()  # Quality between 0.8 and 1.0
                    
                    metadata = {
                        'firmware_version': f"v{random.randint(1,5)}.{random.randint(0,9)}.{random.randint(0,9)}",
                        'battery_level': str(random.randint(20, 100)),
                        'signal_strength': str(random.randint(-80, -30)),
                        'zone': f"zone_{random.randint(1, 10)}"
                    }
                    
                    tags = {f'tag_{random.randint(1, 20)}', sensor_type, 'production'}
                    
                    batch_data.append((
                        device_id, sensor_type, year, month, day, reading_time,
                        value, unit, quality_score, location_lat, location_lng,
                        metadata, tags
                    ))
                    
                    if len(batch_data) >= batch_size:
                        for data in batch_data:
                            self.session.execute(prepared_stmt, data)
                        total_records += len(batch_data)
                        batch_data = []
                        
                        if total_records % 10000 == 0:
                            print(f"    ... {total_records} IoT records inserted")
            
            # Insert remaining batch data
            if batch_data:
                for data in batch_data:
                    self.session.execute(prepared_stmt, data)
                total_records += len(batch_data)
        
        print(f"✅ Generated {total_records} IoT sensor records")
    
    def generate_user_profiles(self, user_count=10000):
        """Generate realistic user profile data"""
        print(f"👥 Generating {user_count} user profiles...")
        
        # Sample data for realistic generation
        first_names = ['John', 'Jane', 'Michael', 'Sarah', 'David', 'Lisa', 'Robert', 'Emily', 
                       'Daniel', 'Jessica', 'William', 'Ashley', 'James', 'Amanda', 'Christopher']
        last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 
                      'Davis', 'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez']
        cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Philadelphia', 'Phoenix',
                  'San Antonio', 'San Diego', 'Dallas', 'San Jose']
        states = ['NY', 'CA', 'IL', 'TX', 'PA', 'AZ', 'FL', 'OH', 'NC', 'MI']
        countries = ['USA', 'Canada', 'Mexico', 'UK', 'France', 'Germany', 'Australia']
        social_platforms = ['twitter', 'facebook', 'instagram', 'linkedin', 'tiktok', 'youtube']
        
        batch_size = 500
        total_records = 0
        
        prepared_stmt = self.session.prepare("""
            INSERT INTO user_profiles (
                user_id, created_at, updated_at, email, username, full_name, birth_date,
                profile_data, addresses, social_profiles, preferences, tags,
                activity_score, last_login, metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)
        
        batch_data = []
        
        for i in range(user_count):
            user_id = uuid.uuid4()
            first_name = random.choice(first_names)
            last_name = random.choice(last_names)
            username = f"{first_name.lower()}.{last_name.lower()}{random.randint(1, 999)}"
            email = f"{username}@example.com"
            full_name = f"{first_name} {last_name}"
            
            # Generate realistic timestamps
            created_days_ago = random.randint(1, 1000)
            created_at = datetime.now() - timedelta(days=created_days_ago)
            updated_at = created_at + timedelta(days=random.randint(0, created_days_ago))
            last_login = updated_at + timedelta(hours=random.randint(-48, 0))
            
            # Birth date (18-80 years old)
            birth_date = datetime.now().date() - timedelta(days=random.randint(18*365, 80*365))
            
            # Profile data
            profile_data = {
                'bio': f'Hello, I am {first_name}! Love technology and data.',
                'location': random.choice(cities),
                'website': f'https://www.{username}.com',
                'job_title': random.choice(['Developer', 'Manager', 'Analyst', 'Designer', 'Engineer']),
                'company': f'{random.choice(["Tech", "Data", "Smart", "Cloud"])} {random.choice(["Corp", "Inc", "LLC", "Solutions"])}'
            }
            
            # Addresses (1-3 addresses)
            addresses = []
            for addr_num in range(random.randint(1, 3)):
                address = {
                    'street': f'{random.randint(100, 9999)} {random.choice(["Main", "Oak", "Park", "First", "Second"])} {random.choice(["St", "Ave", "Blvd", "Dr"])}',
                    'city': random.choice(cities),
                    'state': random.choice(states),
                    'zip_code': f'{random.randint(10000, 99999)}',
                    'country': random.choice(countries),
                    'coordinates': {'lat': random.uniform(25.0, 49.0), 'lng': random.uniform(-125.0, -66.0)}
                }
                addresses.append(address)
            
            # Social profiles (0-3 platforms)
            social_profiles = []
            used_platforms = random.sample(social_platforms, random.randint(0, 3))
            for platform in used_platforms:
                social_profile = {
                    'platform': platform,
                    'username': f'{username}_{platform}',
                    'verified': random.choice([True, False]),
                    'followers_count': random.randint(10, 10000),
                    'metadata': {'created_date': created_at.isoformat(), 'public': str(random.choice([True, False]))}
                }
                social_profiles.append(social_profile)
            
            # Preferences
            preferences = {
                'theme': random.choice(['light', 'dark', 'auto']),
                'language': random.choice(['en', 'es', 'fr', 'de', 'it']),
                'timezone': random.choice(['America/New_York', 'America/Los_Angeles', 'Europe/London', 'Asia/Tokyo']),
                'notifications': random.choice(['all', 'important', 'none']),
                'privacy_level': random.choice(['public', 'friends', 'private'])
            }
            
            # Tags
            tags = set(random.sample(['premium', 'verified', 'beta_tester', 'power_user', 'mobile_user', 
                                     'web_user', 'api_user', 'developer', 'analyst', 'content_creator'], 
                                    random.randint(1, 5)))
            
            # Activity score (0.0 to 1.0)
            activity_score = random.uniform(0.1, 1.0)
            
            # Metadata
            metadata = {
                'signup_source': random.choice(['web', 'mobile', 'api', 'referral']),
                'account_type': random.choice(['free', 'premium', 'enterprise']),
                'last_ip': f'{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}',
                'device_type': random.choice(['desktop', 'mobile', 'tablet']),
                'referrer': random.choice(['google', 'facebook', 'twitter', 'direct', 'email'])
            }
            
            batch_data.append((
                user_id, created_at, updated_at, email, username, full_name, birth_date,
                profile_data, addresses, social_profiles, preferences, tags,
                activity_score, last_login, metadata
            ))
            
            if len(batch_data) >= batch_size:
                for data in batch_data:
                    self.session.execute(prepared_stmt, data)
                total_records += len(batch_data)
                batch_data = []
                
                if total_records % 2000 == 0:
                    print(f"    ... {total_records} user profiles inserted")
        
        # Insert remaining batch data
        if batch_data:
            for data in batch_data:
                self.session.execute(prepared_stmt, data)
            total_records += len(batch_data)
        
        print(f"✅ Generated {total_records} user profiles")
    
    def generate_content_data(self, content_count=5000):
        """Generate content management system data"""
        print(f"📝 Generating {content_count} content items...")
        
        categories = ['blog', 'news', 'documentation', 'tutorial', 'video', 'podcast']
        subcategories = {
            'blog': ['tech', 'lifestyle', 'business', 'personal'],
            'news': ['breaking', 'technology', 'science', 'politics'],
            'documentation': ['api', 'user_guide', 'reference', 'faq'],
            'tutorial': ['beginner', 'intermediate', 'advanced', 'expert'],
            'video': ['educational', 'entertainment', 'review', 'demo'],
            'podcast': ['interview', 'discussion', 'solo', 'panel']
        }
        
        statuses = ['draft', 'published', 'archived', 'deleted']
        
        # Sample content for realistic generation
        sample_titles = [
            "Getting Started with {technology}",
            "Advanced {technology} Techniques",
            "Best Practices for {technology}",
            "Common {technology} Mistakes to Avoid",
            "The Future of {technology}",
            "How to Optimize {technology} Performance",
            "Understanding {technology} Architecture",
            "Migrating to {technology}",
            "Security in {technology}",
            "Testing {technology} Applications"
        ]
        
        technologies = ['Python', 'JavaScript', 'React', 'Node.js', 'Docker', 'Kubernetes', 
                       'AWS', 'MongoDB', 'PostgreSQL', 'Redis', 'GraphQL', 'Microservices']
        
        batch_size = 200
        total_records = 0
        
        prepared_stmt = self.session.prepare("""
            INSERT INTO content_items (
                content_id, category, subcategory, created_at, updated_at, author_id,
                title, content_text, summary, keywords, tags, view_count, like_count,
                comment_count, metadata, attachments, status, published_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)
        
        batch_data = []
        
        for i in range(content_count):
            content_id = uuid.uuid4()
            category = random.choice(categories)
            subcategory = random.choice(subcategories[category])
            
            # Generate timestamps
            created_days_ago = random.randint(1, 365)
            created_at = datetime.now() - timedelta(days=created_days_ago)
            updated_at = created_at + timedelta(days=random.randint(0, min(30, created_days_ago)))
            
            author_id = uuid.uuid4()  # In real system, this would reference user_profiles
            
            # Generate content
            technology = random.choice(technologies)
            title = random.choice(sample_titles).format(technology=technology)
            
            content_text = f"""
            This is a comprehensive guide about {technology}. 
            
            {technology} is a powerful tool that enables developers to build robust applications.
            In this article, we'll explore the key concepts, best practices, and common pitfalls.
            
            ## Introduction
            
            {technology} has gained significant popularity in recent years due to its versatility
            and ease of use. Whether you're a beginner or an experienced developer, understanding
            {technology} can significantly improve your development workflow.
            
            ## Key Features
            
            - High performance and scalability
            - Rich ecosystem and community support
            - Comprehensive documentation
            - Active development and regular updates
            
            ## Best Practices
            
            1. Follow the official style guide
            2. Write comprehensive tests
            3. Use proper error handling
            4. Optimize for performance
            5. Keep dependencies up to date
            
            ## Conclusion
            
            {technology} is an excellent choice for modern development projects.
            With proper understanding and implementation, it can help you build
            amazing applications efficiently.
            """.strip()
            
            summary = f"A comprehensive guide to {technology} covering key concepts, best practices, and implementation details."
            
            # Keywords and tags
            keywords = {technology.lower(), 'programming', 'development', 'tutorial', 'guide'}
            keywords.update(random.sample(['performance', 'security', 'testing', 'deployment', 
                                         'architecture', 'optimization', 'best-practices'], 3))
            
            tags = {
                'language': technology,
                'difficulty': random.choice(['beginner', 'intermediate', 'advanced']),
                'read_time': f"{random.randint(5, 30)} minutes",
                'author_level': random.choice(['junior', 'senior', 'expert'])
            }
            
            # Engagement metrics
            view_count = random.randint(100, 50000)
            like_count = random.randint(0, view_count // 10)
            comment_count = random.randint(0, view_count // 50)
            
            # Metadata
            metadata = {
                'word_count': str(len(content_text.split())),
                'reading_level': random.choice(['elementary', 'middle', 'high', 'college']),
                'seo_score': str(random.randint(60, 100)),
                'featured': str(random.choice([True, False])),
                'monetized': str(random.choice([True, False]))
            }
            
            # Attachments
            attachments = []
            if random.random() < 0.3:  # 30% chance of having attachments
                num_attachments = random.randint(1, 5)
                for j in range(num_attachments):
                    attachment_type = random.choice(['image', 'video', 'document', 'code'])
                    attachments.append(f"{attachment_type}_{j+1}.{random.choice(['jpg', 'png', 'mp4', 'pdf', 'zip'])}")
            
            # Status and publishing
            status = random.choice(statuses)
            published_at = created_at if status == 'published' else None
            
            batch_data.append((
                content_id, category, subcategory, created_at, updated_at, author_id,
                title, content_text, summary, keywords, tags, view_count, like_count,
                comment_count, metadata, attachments, status, published_at
            ))
            
            if len(batch_data) >= batch_size:
                for data in batch_data:
                    self.session.execute(prepared_stmt, data)
                total_records += len(batch_data)
                batch_data = []
                
                if total_records % 1000 == 0:
                    print(f"    ... {total_records} content items inserted")
        
        # Insert remaining batch data
        if batch_data:
            for data in batch_data:
                self.session.execute(prepared_stmt, data)
            total_records += len(batch_data)
        
        print(f"✅ Generated {total_records} content items")
    
    def cleanup(self):
        """Clean up connections"""
        if self.cluster:
            self.cluster.shutdown()
        print("🔌 Disconnected from Cassandra")

def main():
    parser = argparse.ArgumentParser(description='Generate production-scale test datasets for CQLite')
    parser.add_argument('--hosts', nargs='+', default=['cassandra5-seed'], 
                       help='Cassandra contact points')
    parser.add_argument('--keyspace', default='cqlite_production_test',
                       help='Keyspace to use for test data')
    parser.add_argument('--iot-devices', type=int, default=1000,
                       help='Number of IoT devices to simulate')
    parser.add_argument('--iot-days', type=int, default=30,
                       help='Number of days of IoT data to generate')
    parser.add_argument('--users', type=int, default=10000,
                       help='Number of user profiles to generate')
    parser.add_argument('--content', type=int, default=5000,
                       help='Number of content items to generate')
    parser.add_argument('--skip-schemas', action='store_true',
                       help='Skip schema creation (use existing schemas)')
    
    args = parser.parse_args()
    
    generator = ProductionDatasetGenerator(args.hosts, args.keyspace)
    
    try:
        generator.connect()
        
        if not args.skip_schemas:
            generator.create_schemas()
        
        # Import math here since we need it for IoT data generation
        import math
        
        # Generate all datasets
        generator.generate_iot_data(args.iot_devices, args.iot_days)
        generator.generate_user_profiles(args.users)
        generator.generate_content_data(args.content)
        
        print("\n🎉 Production dataset generation complete!")
        print(f"📊 Summary:")
        print(f"   • IoT sensor data: ~{args.iot_devices * args.iot_days * 144:,} records")
        print(f"   • User profiles: {args.users:,} records")
        print(f"   • Content items: {args.content:,} records")
        print(f"   • Total estimated records: ~{args.iot_devices * args.iot_days * 144 + args.users + args.content:,}")
        
    finally:
        generator.cleanup()

if __name__ == '__main__':
    main()