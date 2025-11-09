#!/usr/bin/env python3
"""
Script to recalculate video usage statistics in compilations
Run this script to fix missing usage statistics in your database
"""

import sys
import os
from datetime import datetime, timedelta

# Add the project directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from flask_pymongo import PyMongo
from flask import Flask
from compilation_parser import VideoUsageTracker

# Initialize Flask app for database connection
app = Flask(__name__)
app.config["MONGO_URI"] = "mongodb://localhost:27017/video_database"
mongo = PyMongo(app)

def main():
    """Main function to recalculate usage statistics"""
    print("=" * 60)
    print("VIDEO USAGE STATISTICS RECALCULATION SCRIPT")
    print("=" * 60)
    
    try:
        with app.app_context():
            # Collection references
            videos_collection = mongo.db.videos
            compilations_collection = mongo.db.compilations
            user_compilations_collection = mongo.db.user_compilations
            
            print(f"Connected to MongoDB at: {app.config['MONGO_URI']}")
            print()
            
            # Initialize usage tracker with all three collections
            usage_tracker = VideoUsageTracker(
                compilations_collection, 
                user_compilations_collection, 
                videos_collection
            )
            
            # Get initial statistics
            total_videos = videos_collection.count_documents({})
            total_auto_compilations = compilations_collection.count_documents({})
            total_user_compilations = user_compilations_collection.count_documents({})
            videos_with_stats = videos_collection.count_documents({
                'compilation_usage_stats': {'$exists': True}
            })
            
            print("CURRENT DATABASE STATE:")
            print(f"  Total videos: {total_videos}")
            print(f"  Auto-generated compilations: {total_auto_compilations}")
            print(f"  User-created compilations: {total_user_compilations}")
            print(f"  Videos with usage stats: {videos_with_stats}")
            print()
            
            # Ask for confirmation
            response = input("Proceed with recalculation? (y/N): ").lower().strip()
            if response != 'y':
                print("Operation cancelled.")
                return
            
            print("Starting recalculation...")
            print("-" * 40)
            
            # Run the recalculation
            start_time = datetime.now()
            result = usage_tracker.recalculate_all_stats()
            end_time = datetime.now()
            
            print("-" * 40)
            print("RECALCULATION COMPLETED!")
            print(f"  Time taken: {end_time - start_time}")
            print(f"  Videos updated: {result['updated']}")
            print(f"  Videos cleared: {result['cleared']}")
            print(f"  Total compilation videos: {result['total_compilation_videos']}")
            print()
            
            # Get final statistics
            final_videos_with_stats = videos_collection.count_documents({
                'compilation_usage_stats': {'$exists': True}
            })
            
            print("FINAL DATABASE STATE:")
            print(f"  Videos with usage stats: {final_videos_with_stats}")
            print(f"  Change: {final_videos_with_stats - videos_with_stats:+d}")
            print()
            
            # Show some sample usage statistics
            print("SAMPLE USAGE STATISTICS:")
            sample_videos = list(videos_collection.find({
                'compilation_usage_stats': {'$exists': True}
            }).limit(5))
            
            for video in sample_videos:
                stats = video.get('compilation_usage_stats', {})
                print(f"  {video.get('title', 'Unknown Title')[:50]}...")
                print(f"    Video ID: {video.get('video_id')}")
                print(f"    Total inclusions: {stats.get('total_inclusions', 0)}")
                print(f"    First video count: {stats.get('first_video_count', 0)}")
                print(f"    Auto-compilation usage: {stats.get('auto_compilation_usage', 0)}")
                print(f"    User-compilation usage: {stats.get('user_compilation_usage', 0)}")
                print()
            
            print("=" * 60)
            print("RECALCULATION COMPLETED SUCCESSFULLY!")
            print("=" * 60)
            
    except Exception as e:
        print(f"ERROR: {e}")
        return 1
    
    return 0

def verify_collections():
    """Verify that all required collections exist and have data"""
    print("VERIFYING DATABASE COLLECTIONS:")
    
    with app.app_context():
        collections = {
            'videos': mongo.db.videos,
            'compilations': mongo.db.compilations,
            'user_compilations': mongo.db.user_compilations
        }
        
        for name, collection in collections.items():
            count = collection.count_documents({})
            print(f"  {name}: {count} documents")
            
            if name in ['compilations', 'user_compilations'] and count > 0:
                # Check if compilations have the expected structure
                sample = collection.find_one({})
                if sample and 'timestamps' in sample:
                    timestamp_count = len(sample.get('timestamps', []))
                    has_video_ids = any(
                        'video_id' in ts for ts in sample.get('timestamps', [])
                    )
                    print(f"    Sample has {timestamp_count} timestamps, video_ids present: {has_video_ids}")
        
        print()

def show_usage_distribution():
    """Show distribution of video usage across compilations"""
    print("USAGE DISTRIBUTION ANALYSIS:")
    
    with app.app_context():
        # Get videos with usage stats
        videos_with_stats = list(mongo.db.videos.find({
            'compilation_usage_stats': {'$exists': True}
        }, {
            'title': 1,
            'video_id': 1,
            'compilation_usage_stats': 1
        }).sort('compilation_usage_stats.total_inclusions', -1).limit(10))
        
        if videos_with_stats:
            print("TOP 10 MOST USED VIDEOS:")
            for i, video in enumerate(videos_with_stats, 1):
                stats = video.get('compilation_usage_stats', {})
                total = stats.get('total_inclusions', 0)
                first = stats.get('first_video_count', 0)
                auto = stats.get('auto_compilation_usage', 0)
                user = stats.get('user_compilation_usage', 0)
                
                print(f"  {i:2d}. {video.get('title', 'Unknown')[:40]}...")
                print(f"      Total: {total}, First: {first}, Auto: {auto}, User: {user}")
        else:
            print("  No videos with usage statistics found.")
        
        print()

if __name__ == "__main__":
    print("What would you like to do?")
    print("1. Verify collections")
    print("2. Show usage distribution")
    print("3. Recalculate all usage statistics")
    print("4. All of the above")
    
    choice = input("Enter your choice (1-4): ").strip()
    
    if choice == "1":
        verify_collections()
    elif choice == "2":
        show_usage_distribution()
    elif choice == "3":
        exit_code = main()
        sys.exit(exit_code)
    elif choice == "4":
        verify_collections()
        show_usage_distribution()
        if input("Proceed with recalculation? (y/N): ").lower() == 'y':
            exit_code = main()
            sys.exit(exit_code)
    else:
        print("Invalid choice.")
        sys.exit(1)
