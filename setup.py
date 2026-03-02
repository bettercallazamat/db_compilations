#!/usr/bin/env python3
"""
Setup script for Video Compilation Analytics
This script helps you set up the environment and initialize the database
"""

import os
import sys
import subprocess
from pathlib import Path


def check_python_version():
    """Check if Python version is compatible"""
    if sys.version_info < (3, 7):
        print("❌ Python 3.7 or higher is required")
        return False
    print(f"✅ Python {sys.version.split()[0]} detected")
    return True


def check_mongodb():
    """Check if MongoDB is running"""
    try:
        from pymongo import MongoClient
        mongo_uri = os.environ.get(
            'MONGO_URI', 'mongodb://localhost:27017/video_database')
        client = MongoClient(mongo_uri,
                             serverSelectionTimeoutMS=2000)
        client.server_info()
        print("✅ MongoDB connection successful")
        return True
    except Exception as e:
        print(f"❌ MongoDB connection failed: {e}")
        print("   Please ensure MongoDB is installed and running on localhost:27017")
        return False


def install_requirements():
    """Install Python requirements"""
    print("📦 Installing Python dependencies...")
    try:
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        print("✅ Dependencies installed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to install dependencies: {e}")
        return False


def initialize_database():
    """Initialize database schema"""
    print("🗄️  Initializing database schema...")
    try:
        from db_schema import initialize_database
        initialize_database()
        return True
    except Exception as e:
        print(f"❌ Database initialization failed: {e}")
        return False


def create_sample_templates():
    """Create sample HTML templates directory structure"""
    templates_dir = Path("templates")
    templates_dir.mkdir(exist_ok=True)

    # Create a basic template structure info
    template_files = [
        "base.html",
        "index.html",
        "video_detail.html",
        "edit_video.html",
        "import.html",
        "stats.html",
        "compilations.html",
        "compilation_detail.html",
        "video_usage_stats.html",
        "video_usage_detail.html"
    ]

    readme_content = """# Templates Directory

This directory should contain your Flask HTML templates.

Required template files:
""" + "\n".join(f"- {filename}" for filename in template_files) + """

You can create these templates based on your existing ones, or create new ones 
following Flask templating conventions.

Example base template structure:
```html
<!DOCTYPE html>
<html>
<head>
    <title>Video Analytics</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body>
    <nav class="navbar navbar-expand-lg navbar-dark bg-dark">
        <div class="container">
            <a class="navbar-brand" href="/">Video Analytics</a>
            <div class="navbar-nav">
                <a class="nav-link" href="/">Videos</a>
                <a class="nav-link" href="/compilations">Compilations</a>
                <a class="nav-link" href="/stats">Stats</a>
                <a class="nav-link" href="/video_usage_stats">Usage Stats</a>
            </div>
        </div>
    </nav>
    
    <div class="container mt-4">
        {% block content %}{% endblock %}
    </div>
</body>
</html>
```
"""

    with open(templates_dir / "README.md", "w") as f:
        f.write(readme_content)

    print(f"✅ Created templates directory structure in {templates_dir}")


def run_tests():
    """Run basic functionality tests"""
    print("🧪 Running basic functionality tests...")

    try:
        # Test compilation parser
        from compilation_parser import CompilationParser

        # Test with sample description
        test_description = """
        Subscribe to D Billions Kids Songs: https://example.com
        
        Mega Compilation of fun songs!
        
        Timestamps:
        0:00 First Song
        3:30 Second Song  
        7:45 Third Song + MORE
        """

        # Test compilation detection
        is_comp = CompilationParser.is_compilation(test_description)
        if not is_comp:
            print("❌ Compilation detection test failed")
            return False

        # Test timestamp parsing
        timestamps = CompilationParser.parse_timestamps(test_description)
        if len(timestamps) != 3:
            print(
                f"❌ Timestamp parsing test failed: expected 3, got {len(timestamps)}")
            return False

        # Test duration rounding
        rounded = CompilationParser.round_duration_to_nearest_5min(
            660)  # 11 minutes
        if rounded != 10:
            print(
                f"❌ Duration rounding test failed: expected 10, got {rounded}")
            return False

        print("✅ All basic functionality tests passed")
        return True

    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        return False


def create_sample_data():
    """Create some sample data for testing"""
    print("📊 Creating sample test data...")

    try:
        from pymongo import MongoClient
        from datetime import datetime

        client = MongoClient('mongodb://localhost:27017/')
        db = client.video_database

        # Sample video data
        sample_videos = [
            {
                'title': 'Fun Kids Song Collection - Mega Compilation',
                'video_id': 'sample_comp_001',
                'published_at': '2024-01-15',
                'description': '''Subscribe to our channel: https://example.com
                
                Mega Compilation of the best kids songs!
                
                Timestamps:
                0:00 Happy Birthday Song
                3:20 ABC Song
                6:45 Twinkle Twinkle Little Star
                9:30 Old MacDonald + MORE
                ''',
                'thumbnail_url': 'https://example.com/thumb1.jpg',
                'duration': '12:30',
                'duration_seconds': 750,
                'view_count': 1000000,
                'like_count': 15000,
                'comment_count': 500,
                'estimated_minutes_watched': 500000,
                'average_view_duration': 300,
                'average_view_percentage': 40,
                'retention_30s': 85,
                'actor': False,
                'tags': ['compilation', 'kids', 'songs'],
                'is_compilation': False,  # Will be updated by processing
                'compilation_usage_stats': {},
                'created_at': datetime.utcnow(),
                'updated_at': datetime.utcnow()
            },
            {
                'title': 'Happy Birthday Song',
                'video_id': 'sample_vid_001',
                'published_at': '2023-12-01',
                'description': 'A fun happy birthday song for kids',
                'thumbnail_url': 'https://example.com/thumb2.jpg',
                'duration': '3:20',
                'duration_seconds': 200,
                'view_count': 500000,
                'like_count': 8000,
                'comment_count': 200,
                'estimated_minutes_watched': 100000,
                'average_view_duration': 120,
                'average_view_percentage': 60,
                'retention_30s': 90,
                'actor': True,
                'tags': ['birthday', 'kids', 'celebration'],
                'is_compilation': False,
                'compilation_usage_stats': {},
                'created_at': datetime.utcnow(),
                'updated_at': datetime.utcnow()
            },
            {
                'title': 'ABC Song',
                'video_id': 'sample_vid_002',
                'published_at': '2023-11-15',
                'description': 'Learn the alphabet with this fun ABC song',
                'thumbnail_url': 'https://example.com/thumb3.jpg',
                'duration': '3:25',
                'duration_seconds': 205,
                'view_count': 750000,
                'like_count': 12000,
                'comment_count': 300,
                'estimated_minutes_watched': 150000,
                'average_view_duration': 150,
                'average_view_percentage': 73,
                'retention_30s': 88,
                'actor': True,
                'tags': ['alphabet', 'education', 'kids'],
                'is_compilation': False,
                'compilation_usage_stats': {},
                'created_at': datetime.utcnow(),
                'updated_at': datetime.utcnow()
            }
        ]

        # Insert sample videos (skip if they already exist)
        for video in sample_videos:
            existing = db.videos.find_one({'video_id': video['video_id']})
            if not existing:
                db.videos.insert_one(video.copy())

        print("✅ Sample data created successfully")
        print("   You can now test the compilation processing functionality")
        return True

    except Exception as e:
        print(f"❌ Failed to create sample data: {e}")
        return False


def show_next_steps():
    """Show next steps after setup"""
    print("\n🎉 Setup completed successfully!")
    print("\n📋 Next steps:")
    print("1. Create your HTML templates in the 'templates' directory")
    print("2. Start the Flask application:")
    print("   python parse_json_updated.py")
    print("3. Open your browser and go to: http://localhost:5002")
    print("4. Import your JSON data using the /import endpoint")
    print("5. Process compilations using the /process_compilations endpoint")
    print("\n🛠️  Useful commands:")
    print("   python utils.py init          # Initialize database")
    print("   python utils.py analyze       # Analyze compilation keywords")
    print("   python utils.py report        # Generate usage report")
    print("   python utils.py validate      # Validate data")
    print("\n📖 File structure:")
    print("   parse_json_updated.py         # Main Flask application")
    print("   compilation_parser.py         # Compilation parsing logic")
    print("   compilation_manager.py        # Compilation management")
    print("   db_schema.py                  # Database schema management")
    print("   utils.py                      # Utility functions")
    print("   requirements.txt              # Python dependencies")


def main():
    """Main setup function"""
    print("🚀 DB Compilations Setup")
    print("=" * 50)

    # Check prerequisites
    if not check_python_version():
        return False

    if not check_mongodb():
        return False

    # Install dependencies
    if not install_requirements():
        return False

    # Initialize database
    if not initialize_database():
        return False

    # Create template structure
    # create_sample_templates()

    # Run tests
    if not run_tests():
        return False

    # Create sample data
    # create_sample_data()

    # Show next steps
    show_next_steps()

    return True


if __name__ == '__main__':
    success = main()
    if not success:
        print("\n❌ Setup failed. Please check the errors above and try again.")
        sys.exit(1)
    else:
        print("\n✅ Setup completed successfully!")
