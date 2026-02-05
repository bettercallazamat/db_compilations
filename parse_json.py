from enum import Enum
import math
from typing import List, Dict, Tuple, Optional
from flask import Flask, render_template, request, jsonify, redirect, url_for, send_file, Blueprint, current_app, session
from flask_pymongo import PyMongo
from bson.objectid import ObjectId
import json
import logging
from datetime import datetime, timedelta
import os
import re

# Import our enhanced modules
from compilation_manager import CompilationManager
from compilation_parser import CompilationParser, VideoUsageTracker
from compilation_creator import CompilationCreator, CompilationStatus, VideoCategory
from export_manager import CompilationExporter
from frontend_manager import FrontendTemplateManager

app = Flask(__name__)

# Flask configuration
app.config["MONGO_URI"] = "mongodb://localhost:27017/video_database"
app.config["SECRET_KEY"] = "your-secret-key-change-in-production"

# Session timeout (60 minutes)
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(minutes=60)

# Initialize extensions
mongo = PyMongo(app)

# Collection references
videos_collection = mongo.db.videos
compilations_collection = mongo.db.compilations
user_compilations_collection = mongo.db.user_compilations  # New collection for user-created compilations
blacklist_collection = mongo.db.video_blacklist  # Collection for blacklisted videos
channels_collection = mongo.db.channels  # Collection for channel information

# Initialize managers with enhanced functionality
compilation_manager = CompilationManager(
    videos_collection,
    compilations_collection,
    user_compilations_collection  # Add this third parameter
)
compilation_creator = CompilationCreator(videos_collection, compilations_collection, user_compilations_collection, blacklist_collection)
compilation_exporter = CompilationExporter(user_compilations_collection, videos_collection)
frontend_manager = FrontendTemplateManager()


# ==================== CUSTOM JINJA2 FILTERS ====================

def format_published_date(date_string):
    """
    Format published date in DD.MM.YYYY HH:MM format
    Handles various input formats including ISO 8601, simple date, and empty strings
    """
    if not date_string or date_string.strip() == '':
        return 'Unknown'
    
    try:
        # Handle ISO 8601 format (e.g., '2024-09-15T10:00:00Z')
        if 'T' in date_string:
            # Remove 'Z' and parse as UTC, then convert to local time
            dt = datetime.fromisoformat(date_string.replace('Z', '+00:00'))
            # Convert to local time (assuming UTC+6 for Asia/Bishkek)
            local_dt = dt.replace(tzinfo=None) + timedelta(hours=6)
            return local_dt.strftime('%d.%m.%Y %H:%M')
        
        # Handle simple date format (e.g., '2024-09-15')
        elif '-' in date_string and len(date_string) >= 10:
            dt = datetime.strptime(date_string[:10], '%Y-%m-%d')
            return dt.strftime('%d.%m.%Y')
        
        # Try other common formats
        else:
            # Try parsing as various formats
            for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M', '%d.%m.%Y', '%m/%d/%Y']:
                try:
                    dt = datetime.strptime(date_string, fmt)
                    if fmt == '%Y-%m-%d %H:%M:%S' or fmt == '%Y-%m-%d %H:%M':
                        return dt.strftime('%d.%m.%Y %H:%M')
                    else:
                        return dt.strftime('%d.%m.%Y')
                except ValueError:
                    continue
            
            # If no format works, return original
            return date_string
            
    except Exception as e:
        # If parsing fails, return the original string
        return date_string


# Register the custom filter
app.jinja_env.filters['format_date'] = format_published_date


# ==================== AUTHENTICATION SYSTEM ====================

def login_required(f):
    """Decorator to require authentication for protected routes"""
    from functools import wraps

    @wraps(f)
    def decorated_function(*args, **kwargs):
        # Check if user is logged in
        if 'logged_in' not in session or not session.get('logged_in'):
            return redirect(url_for('login'))

        # Check if session has expired
        last_activity = session.get('last_activity')
        if last_activity:
            # Check if more than 60 minutes have passed
            if datetime.now() - datetime.fromisoformat(last_activity) > timedelta(minutes=60):
                # Session expired, clear session and redirect to login
                session.clear()
                return redirect(url_for('login', expired=True))

        # Update last activity time
        session['last_activity'] = datetime.now().isoformat()

        return f(*args, **kwargs)

    return decorated_function


@app.route('/login', methods=['GET', 'POST'])
def login():
    """Login page with simple authentication"""
    error = None

    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '')

        # Simple authentication: username="admin", password="135792468"
        if username == 'admin' and password == '135792468':
            # Set session
            session.permanent = True  # Use permanent session
            session['logged_in'] = True
            session['username'] = 'admin'
            session['last_activity'] = datetime.now().isoformat()
            
            # Set current channel to first available channel
            channels = ChannelManager.get_all_channels()
            if channels:
                session['current_channel_id'] = channels[0].get('channel_id')

            # Redirect to the page user was trying to access or home
            next_url = request.args.get('next')
            if next_url and next_url.startswith('/'):
                return redirect(next_url)
            else:
                return redirect(url_for('index'))
        else:
            error = "Invalid username or password"

    return render_template('login.html', error=error)


@app.route('/logout')
@login_required
def logout():
    """Logout and clear session"""
    session.clear()
    return redirect(url_for('login', logged_out=True))


class VideoManager:
    @staticmethod
    def import_from_json(json_file_path):
        """Import videos from JSON file with enhanced error handling and processing"""
        try:
            with open(json_file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)

            videos = data.get('videos', [])
            imported_count = 0

            for video_data in videos:
                # Check if video already exists
                existing = videos_collection.find_one(
                    {'video_id': video_data['video_id']})
                if existing:
                    continue

                # Prepare enhanced video document with new compilation tracking fields
                video_doc = {
                    'title': video_data.get('title', ''),
                    'video_id': video_data.get('video_id', ''),
                    'published_at': video_data.get('published_at', ''),
                    'description': video_data.get('description', ''),
                    'thumbnail_url': video_data.get('thumbnail_url', ''),
                    'duration': video_data.get('duration', ''),
                    'duration_seconds': video_data.get('duration_seconds', 0),
                    'view_count': video_data.get('view_count', 0),
                    'like_count': video_data.get('like_count', 0),
                    'comment_count': video_data.get('comment_count', 0),
                    'estimated_minutes_watched': video_data.get('estimated_minutes_watched', 0),
                    'average_view_duration': video_data.get('average_view_duration', 0),
                    'average_view_percentage': video_data.get('average_view_percentage', 0),
                    'retention_30s': video_data.get('retention_30s', 0),
                    # Enhanced fields for compilation management
                    'actor': False,
                    'tags': [],
                    'is_compilation': False,
                    'compilation_usage_stats': {},
                    'user_compilation_usage': {
                        'total_inclusions': 0,
                        'last_used': None,
                        'usage_by_duration': {},
                        'first_video_count': 0
                    },
                    'created_at': datetime.utcnow(),
                    'updated_at': datetime.utcnow()
                }

                videos_collection.insert_one(video_doc)
                imported_count += 1

            return imported_count, len(videos)

        except Exception as e:
            return 0, 0, str(e)

    @staticmethod
    def enhanced_import_from_json(json_file_path, skip_existing=True, update_existing=False, validate_data=True):
        """Enhanced import with validation and better error handling.
        
        Supports both single-channel format (videos array) and multi-channel format (channels object).
        """
        try:
            with open(json_file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)

            # Detect JSON format: check for channels key (multi-channel) or videos key (single-channel)
            has_channels = 'channels' in data and isinstance(data['channels'], dict)
            has_videos = 'videos' in data and isinstance(data['videos'], list)

            if not has_channels and not has_videos:
                return {'error': 'Invalid JSON format: missing "channels" object or "videos" array'}

            # Handle multi-channel format
            if has_channels:
                return VideoManager._import_multi_channel_json(
                    data['channels'],
                    skip_existing=skip_existing,
                    update_existing=update_existing,
                    validate_data=validate_data
                )
            
            # Handle single-channel format (legacy)
            else:
                return VideoManager._import_single_channel_json(
                    data.get('videos', []),
                    skip_existing=skip_existing,
                    update_existing=update_existing,
                    validate_data=validate_data
                )

        except json.JSONDecodeError as e:
            return {'error': f'Invalid JSON file: {str(e)}'}
        except Exception as e:
            return {'error': f'Import failed: {str(e)}'}

    @staticmethod
    def _import_multi_channel_json(channels_data, skip_existing=True, update_existing=False, validate_data=True):
        """Import videos from multi-channel JSON format.
        
        Args:
            channels_data: Dictionary of channels with channel_name -> channel_data mapping
            skip_existing: Skip videos that already exist
            update_existing: Update existing videos with new data
            validate_data: Validate video data before import
        """
        imported_count = 0
        updated_count = 0
        skipped_count = 0
        total_videos = 0
        channels_processed = 0
        channels_updated = 0
        errors = []

        print(f"🔄 Starting multi-channel import...")
        print(f"   Found {len(channels_data)} channels")
        print(f"   Options: skip_existing={skip_existing}, update_existing={update_existing}")

        for channel_name, channel_info in channels_data.items():
            try:
                channel_id = channel_info.get('channel_id', '')
                channel_statistics = channel_info.get('channel_statistics', {})
                videos = channel_info.get('videos', [])

                # Update/insert channel information
                channel_doc = ChannelManager.upsert_channel(
                    channel_id=channel_id,
                    channel_name=channel_name,
                    channel_statistics=channel_statistics
                )
                if channel_doc:
                    channels_updated += 1

                if not videos:
                    continue

                total_videos += len(videos)
                channels_processed += 1

                print(f"   📺 Processing channel: {channel_name} ({len(videos)} videos)")

                # Collect video data for this channel
                video_data_dict = {}
                for i, video_data in enumerate(videos):
                    try:
                        # Add channel info to video data
                        video_data['channel_id'] = channel_id
                        video_data['channel_name'] = channel_name

                        # Validation
                        if validate_data:
                            validation_result = VideoManager._validate_video_data(
                                video_data, i)
                            if validation_result['error']:
                                errors.append(f"{channel_name} - {validation_result['error']}")
                                continue

                        video_id = video_data.get('video_id', '')
                        if not video_id:
                            errors.append(f"{channel_name} - Video {i+1}: Missing video_id")
                            continue

                        video_data_dict[video_id] = video_data

                    except Exception as video_error:
                        errors.append(f"{channel_name} - Video {i+1}: {str(video_error)}")
                        continue

                # Mark videos as deleted for this channel
                deleted_videos = VideoManager.mark_deleted_videos(
                    video_data_dict, channel_id=channel_id)

                # Process videos for this channel
                for video_id, video_data in video_data_dict.items():
                    try:
                        existing = videos_collection.find_one({'video_id': video_id})

                        if existing:
                            if skip_existing and not update_existing:
                                skipped_count += 1
                                continue
                            elif update_existing:
                                update_doc = VideoManager._prepare_video_update(
                                    video_data, channel_id=channel_id, channel_name=channel_name)
                                result = videos_collection.update_one(
                                    {'video_id': video_id},
                                    {'$set': update_doc}
                                )
                                if result.modified_count > 0:
                                    updated_count += 1
                                    imported_count += 1
                                continue
                            else:
                                skipped_count += 1
                                continue

                        # Prepare new video document with channel info
                        video_doc = VideoManager._prepare_video_document(
                            video_data, channel_id=channel_id, channel_name=channel_name)
                        videos_collection.insert_one(video_doc)
                        imported_count += 1

                    except Exception as video_error:
                        errors.append(f'{channel_name} - Video {video_id}: {str(video_error)}')
                        continue

            except Exception as channel_error:
                errors.append(f'Channel {channel_name}: {str(channel_error)}')
                continue

        print(f"   Multi-channel import completed:")
        print(f"     - Channels processed: {channels_processed}")
        print(f"     - Channels updated: {channels_updated}")
        print(f"     - Total videos: {total_videos}")
        print(f"     - New videos: {imported_count - updated_count}")
        print(f"     - Updated videos: {updated_count}")
        print(f"     - Skipped videos: {skipped_count}")
        print(f"     - Errors: {len(errors)}")

        # Process all compilations AFTER importing videos
        print("   🔄 Processing all compilations...")
        processing_results = compilation_manager.process_all_compilations()
        print(f"   ✅ Processing completed:")
        print(f"     - Videos processed: {processing_results['processed']}")
        print(f"     - New compilations: {processing_results['new_compilations']}")
        print(f"     - Updated compilations: {processing_results['updated_compilations']}")
        if processing_results['errors']:
            print(f"     - Compilation errors: {len(processing_results['errors'])}")

        # Recalculate all statistics comprehensively
        print("   📊 Recalculating all usage statistics...")
        tracker = VideoUsageTracker(
            compilations_collection, user_compilations_collection, videos_collection)
        stats_result = tracker.recalculate_all_stats()
        print(f"   ✅ Statistics recalculated:")
        print(f"     - Videos processed: {stats_result.get('videos_processed', 'N/A')}")
        print(f"     - Compilations processed: {stats_result.get('compilations_processed', 'N/A')}")
        print(f"     - Statistics updated: {stats_result.get('stats_updated', 'N/A')}")

        return {
            'imported': imported_count,
            'updated': updated_count,
            'total': total_videos,
            'skipped': skipped_count,
            'deleted_count': len(deleted_videos) if 'deleted_videos' in locals() else 0,
            'channels_processed': channels_processed,
            'channels_updated': channels_updated,
            'processing_results': processing_results,
            'stats_result': stats_result,
            'format': 'multi-channel',
            'errors': errors[:20]
        }

    @staticmethod
    def _import_single_channel_json(videos, skip_existing=True, update_existing=False, validate_data=True):
        """Import videos from single-channel JSON format (legacy format).
        
        Assigns default channel "DB Main Channel" to imported videos.
        """
        imported_count = 0
        updated_count = 0
        skipped_count = 0
        errors = []
        default_channel_id = ''
        default_channel_name = 'DB Main Channel'

        print(f"🔄 Starting single-channel import (legacy format)...")
        print(f"   Found {len(videos)} videos")
        print(f"   Options: skip_existing={skip_existing}, update_existing={update_existing}")

        if not videos:
            return {'error': 'No videos found in the JSON file'}

        # Collect video data first
        video_data_dict = {}
        for i, video_data in enumerate(videos):
            try:
                # Add default channel info for legacy format
                video_data['channel_id'] = default_channel_id
                video_data['channel_name'] = default_channel_name

                # Validation
                if validate_data:
                    validation_result = VideoManager._validate_video_data(
                        video_data, i)
                    if validation_result['error']:
                        errors.append(validation_result['error'])
                        continue

                video_id = video_data.get('video_id', '')
                if not video_id:
                    errors.append(f'Video {i+1}: Missing video_id')
                    continue

                video_data_dict[video_id] = video_data

            except Exception as video_error:
                errors.append(f'Video {i+1}: {str(video_error)}')
                continue

        print(f"   Validated {len(video_data_dict)} videos for processing")

        # Mark videos as deleted that exist in DB but not in new JSON
        print("   🔍 Checking for deleted videos...")
        deleted_videos = VideoManager.mark_deleted_videos(video_data_dict)
        if deleted_videos:
            print(f"   📝 Found {len(deleted_videos)} deleted videos")
            for video in deleted_videos[:5]:  # Show first 5 examples
                print(f"     - {video['original_title']} → {video['new_title']}")
            if len(deleted_videos) > 5:
                print(f"     ... and {len(deleted_videos) - 5} more")

        # Process videos efficiently
        for video_id, video_data in video_data_dict.items():
            try:
                # Check if video already exists
                existing = videos_collection.find_one({'video_id': video_id})

                if existing:
                    if skip_existing and not update_existing:
                        skipped_count += 1
                        continue
                    elif update_existing:
                        # Update existing video
                        update_doc = VideoManager._prepare_video_update(
                            video_data, channel_id=default_channel_id, channel_name=default_channel_name)
                        result = videos_collection.update_one(
                            {'video_id': video_id},
                            {'$set': update_doc}
                        )
                        if result.modified_count > 0:
                            updated_count += 1
                            imported_count += 1
                        continue
                    else:
                        skipped_count += 1
                        continue

                # Prepare new video document
                video_doc = VideoManager._prepare_video_document(
                    video_data, channel_id=default_channel_id, channel_name=default_channel_name)
                videos_collection.insert_one(video_doc)
                imported_count += 1

            except Exception as video_error:
                errors.append(f'Video {video_id}: {str(video_error)}')
                continue

        print(f"   Import completed:")
        print(f"     - New videos: {imported_count - updated_count}")
        print(f"     - Updated videos: {updated_count}")
        print(f"     - Skipped videos: {skipped_count}")
        print(f"     - Errors: {len(errors)}")

        # Process all compilations AFTER importing videos
        print("   🔄 Processing all compilations...")
        processing_results = compilation_manager.process_all_compilations()
        print(f"   ✅ Processing completed:")
        print(f"     - Videos processed: {processing_results['processed']}")
        print(f"     - New compilations: {processing_results['new_compilations']}")
        print(f"     - Updated compilations: {processing_results['updated_compilations']}")
        if processing_results['errors']:
            print(f"     - Compilation errors: {len(processing_results['errors'])}")

        # Recalculate all statistics comprehensively
        print("   📊 Recalculating all usage statistics...")
        tracker = VideoUsageTracker(
            compilations_collection, user_compilations_collection, videos_collection)
        stats_result = tracker.recalculate_all_stats()
        print(f"   ✅ Statistics recalculated:")
        print(f"     - Videos processed: {stats_result.get('videos_processed', 'N/A')}")
        print(f"     - Compilations processed: {stats_result.get('compilations_processed', 'N/A')}")
        print(f"     - Statistics updated: {stats_result.get('stats_updated', 'N/A')}")

        return {
            'imported': imported_count,
            'updated': updated_count,
            'total': len(videos),
            'skipped': skipped_count,
            'deleted_count': len(deleted_videos),
            'deleted_videos': deleted_videos,
            'processing_results': processing_results,
            'stats_result': stats_result,
            'format': 'single-channel (legacy)',
            'errors': errors[:20]
        }

    @staticmethod
    def _validate_video_data(video_data, index):
        """Validate individual video data"""
        required_fields = ['video_id']

        for field in required_fields:
            if not video_data.get(field):
                return {'error': f'Video {index+1}: Missing required field "{field}"'}

        # Validate data types
        numeric_fields = ['duration_seconds', 'view_count', 'like_count', 'comment_count',
                          'estimated_minutes_watched', 'average_view_duration',
                          'average_view_percentage', 'retention_30s']

        for field in numeric_fields:
            if field in video_data and video_data[field] is not None:
                try:
                    float(video_data[field])
                except (ValueError, TypeError):
                    return {'error': f'Video {index+1}: Invalid numeric value for "{field}"'}

        return {'error': None}

    @staticmethod
    def _prepare_video_document(video_data, channel_id='', channel_name=''):
        """Prepare video document for insertion"""
        return {
            'title': video_data.get('title', ''),
            'video_id': video_data.get('video_id', ''),
            'published_at': video_data.get('published_at', ''),
            'description': video_data.get('description', ''),
            'thumbnail_url': video_data.get('thumbnail_url', ''),
            'duration': video_data.get('duration', ''),
            'duration_seconds': int(video_data.get('duration_seconds', 0)) if video_data.get('duration_seconds') else 0,
            'view_count': int(video_data.get('view_count', 0)) if video_data.get('view_count') else 0,
            'like_count': int(video_data.get('like_count', 0)) if video_data.get('like_count') else 0,
            'comment_count': int(video_data.get('comment_count', 0)) if video_data.get('comment_count') else 0,
            'estimated_minutes_watched': int(video_data.get('estimated_minutes_watched', 0)) if video_data.get('estimated_minutes_watched') else 0,
            'average_view_duration': float(video_data.get('average_view_duration', 0)) if video_data.get('average_view_duration') else 0,
            'average_view_percentage': float(video_data.get('average_view_percentage', 0)) if video_data.get('average_view_percentage') else 0,
            'retention_30s': float(video_data.get('retention_30s', 0)) if video_data.get('retention_30s') else 0,
            # Channel information
            'channel_id': channel_id or video_data.get('channel_id', ''),
            'channel_name': channel_name or video_data.get('channel_name', 'Unknown'),
            # Enhanced fields for compilation management
            'actor': False,
            'tags': [],
            'is_compilation': False,
            'is_deleted': False,  # New field to track deleted videos
            'compilation_usage_stats': {},
            'user_compilation_usage': {
                'total_inclusions': 0,
                'last_used': None,
                'usage_by_duration': {},
                'first_video_count': 0
            },
            'created_at': datetime.utcnow(),
            'updated_at': datetime.utcnow()
        }

    @staticmethod
    def _prepare_video_update(video_data, channel_id='', channel_name=''):
        """Prepare update document for existing videos"""
        update_doc = {
            'title': video_data.get('title', ''),
            'published_at': video_data.get('published_at', ''),
            'description': video_data.get('description', ''),
            'thumbnail_url': video_data.get('thumbnail_url', ''),
            'duration': video_data.get('duration', ''),
            'updated_at': datetime.utcnow()
        }

        # Update channel fields
        if channel_id:
            update_doc['channel_id'] = channel_id
        if channel_name:
            update_doc['channel_name'] = channel_name

        # Only update numeric fields if they have valid values
        numeric_fields = {
            'duration_seconds': int,
            'view_count': int,
            'like_count': int,
            'comment_count': int,
            'estimated_minutes_watched': int,
            'average_view_duration': float,
            'average_view_percentage': float,
            'retention_30s': float
        }

        for field, data_type in numeric_fields.items():
            if field in video_data and video_data[field] is not None:
                try:
                    update_doc[field] = data_type(video_data[field])
                except (ValueError, TypeError):
                    pass  # Skip invalid values

        return update_doc

    @staticmethod
    def mark_deleted_videos(video_data_dict, channel_id=None):
        """Mark videos as deleted that exist in DB but not in new JSON data.
        
        Args:
            video_data_dict: Dictionary of video_id -> video_data mappings
            channel_id: Optional channel ID to scope deletion to (for multi-channel)
        """
        try:
            # Get all video IDs from the new JSON data
            new_video_ids = set(video_data_dict.keys())
            
            # Build query to find videos not in new JSON
            query = {'video_id': {'$nin': list(new_video_ids)}}
            
            # If channel_id is provided, only mark videos from that channel as deleted
            if channel_id:
                query['channel_id'] = channel_id
            
            # Find videos in database that are not in new JSON
            deleted_videos = []
            existing_videos = list(videos_collection.find(query))
            
            for video in existing_videos:
                video_id = video.get('video_id')
                current_title = video.get('title', '')
                
                # Skip if already marked as deleted
                if video.get('is_deleted', False):
                    continue
                    
                # Skip if title already has [DELETED] prefix
                if current_title.startswith('[DELETED]'):
                    continue
                
                # Update video to mark as deleted
                new_title = f"[DELETED] {current_title}"
                update_result = videos_collection.update_one(
                    {'video_id': video_id},
                    {
                        '$set': {
                            'title': new_title,
                            'is_deleted': True,
                            'updated_at': datetime.utcnow()
                        }
                    }
                )
                
                if update_result.modified_count > 0:
                    deleted_videos.append({
                        'video_id': video_id,
                        'original_title': current_title,
                        'new_title': new_title
                    })
            
            print(f"   🗑️  Marked {len(deleted_videos)} videos as deleted")
            return deleted_videos
            
        except Exception as e:
            print(f"   ⚠️  Error marking deleted videos: {e}")
            return []


# ==================== CHANNEL SELECTION ====================

@app.route('/set-channel/<channel_id>')
def set_channel(channel_id):
    """Switch the current channel and redirect back to the referring page"""
    # Verify the channel exists
    channel = ChannelManager.get_channel_by_id(channel_id)
    if channel:
        session['current_channel_id'] = channel_id
    
    # Get the referring page or default to index
    referrer = request.headers.get('Referer', '/')
    
    # Basic security: only allow redirects to our own URLs
    if referrer.startswith(request.host_url):
        return redirect(referrer)
    else:
        return redirect(url_for('index'))


@app.context_processor
def inject_channel_context():
    """Make channel information available to all templates"""
    # Get all available channels
    available_channels = ChannelManager.get_all_channels()
    
    # Get current channel from session, default to first available
    current_channel_id = session.get('current_channel_id')
    
    if not current_channel_id and available_channels:
        # Set session to first channel if not already set
        current_channel_id = available_channels[0].get('channel_id')
        session['current_channel_id'] = current_channel_id
    
    current_channel = None
    if current_channel_id:
        current_channel = ChannelManager.get_channel_by_id(current_channel_id)
    
    return {
        'available_channels': available_channels,
        'current_channel': current_channel
    }


def get_current_channel_id():
    """Helper function to get current channel ID for queries"""
    if 'current_channel_id' not in session:
        # Get first available channel and set it
        channels = ChannelManager.get_all_channels()
        if channels:
            session['current_channel_id'] = channels[0].get('channel_id')
        else:
            return None
    return session.get('current_channel_id')


# ==================== CHANNEL MANAGER ===================-

class ChannelManager:
    """Manages channel information in the database."""

    @staticmethod
    def upsert_channel(channel_id, channel_name, channel_statistics=None):
        """Update or insert channel information.
        
        Args:
            channel_id: YouTube channel ID
            channel_name: Channel display name
            channel_statistics: Optional dictionary with channel statistics
            
        Returns:
            The channel document that was upserted
        """
        try:
            if not channel_id:
                return None

            # Build channel document
            channel_doc = {
                'channel_id': channel_id,
                'channel_name': channel_name,
                'channel_statistics': channel_statistics or {},
                'last_updated': datetime.utcnow()
            }

            # Upsert: update if exists, insert if not
            result = channels_collection.update_one(
                {'channel_id': channel_id},
                {
                    '$set': channel_doc,
                    '$setOnInsert': {
                        'created_at': datetime.utcnow()
                    }
                },
                upsert=True
            )

            if result.upserted_id:
                print(f"   📺 Created new channel: {channel_name}")
            else:
                print(f"   📺 Updated channel: {channel_name}")

            # Return the channel document
            return channels_collection.find_one({'channel_id': channel_id})

        except Exception as e:
            print(f"   ⚠️  Error upserting channel {channel_name}: {e}")
            return None

    @staticmethod
    def get_all_channels():
        """Get all channels from the database."""
        try:
            return list(channels_collection.find({}))
        except Exception as e:
            print(f"   ⚠️  Error getting channels: {e}")
            return []

    @staticmethod
    def get_channel_by_id(channel_id):
        """Get a channel by its ID."""
        try:
            return channels_collection.find_one({'channel_id': channel_id})
        except Exception as e:
            print(f"   ⚠️  Error getting channel {channel_id}: {e}")
            return None

    @staticmethod
    def get_channel_by_name(channel_name):
        """Get a channel by its name."""
        try:
            return channels_collection.find_one({'channel_name': channel_name})
        except Exception as e:
            print(f"   ⚠️  Error getting channel {channel_name}: {e}")
            return None

    @staticmethod
    def delete_channel(channel_id):
        """Delete a channel from the database."""
        try:
            result = channels_collection.delete_one({'channel_id': channel_id})
            return result.deleted_count > 0
        except Exception as e:
            print(f"   ⚠️  Error deleting channel {channel_id}: {e}")
            return False

    @staticmethod
    def update_channel_stats(channel_id, channel_statistics):
        """Update channel statistics."""
        try:
            result = channels_collection.update_one(
                {'channel_id': channel_id},
                {
                    '$set': {
                        'channel_statistics': channel_statistics,
                        'last_updated': datetime.utcnow()
                    }
                }
            )
            return result.modified_count > 0
        except Exception as e:
            print(f"   ⚠️  Error updating channel stats {channel_id}: {e}")
            return False

# ==================== ENHANCED ORIGINAL ROUTES ====================

@app.route('/')
@login_required
def index():
    """Enhanced main page with improved filtering and compilation insights"""
    page = request.args.get('page', 1, type=int)
    per_page = 12  # Increased for better UX

    # Enhanced filter parameters
    search_query = request.args.get('search', '')
    actor_filter = request.args.get('actor')
    compilation_filter = request.args.get('compilation')
    retention_filter = request.args.get('retention')  # New filter
    tag_filter = request.args.get('tag')
    
    # Get sort parameters
    sort_column = request.args.get('sort', '')
    sort_order = request.args.get('order', 'desc')
    
    # Build precise search query focused on titles
    query = {}
    if search_query:
        # Escape special regex characters for safe searching
        import re
        escaped_query = re.escape(search_query.strip())

        # Search primarily in title (most relevant for users)
        # and optionally in video ID (for power users)
        query['$or'] = [
            # Exact word match in title (case insensitive)
            {'title': {'$regex': f'\\b{escaped_query}\\b', '$options': 'i'}},
            # Title contains search term (case insensitive)
            {'title': {'$regex': escaped_query, '$options': 'i'}},
            # Video ID exact match (for specific video searches)
            {'video_id': {'$regex': escaped_query, '$options': 'i'}},
        ]

    if actor_filter == 'true':
        query['actor'] = True
    elif actor_filter == 'false':
        query['actor'] = False

    if compilation_filter == 'true':
        query['is_compilation'] = True
    elif compilation_filter == 'false':
        query['is_compilation'] = False
    
    # Retention rate filtering using retention_30s field
    if retention_filter:
        if retention_filter == 'high':
            query['retention_30s'] = {'$gte': 70}
        elif retention_filter == 'medium':
            query['retention_30s'] = {'$gte': 50, '$lt': 70}
        elif retention_filter == 'low':
            query['retention_30s'] = {'$lt': 50}

    # Tag filtering
    if tag_filter:
        query['tags'] = tag_filter
    
    # Channel filtering - only show videos from current channel
    current_channel_id = get_current_channel_id()
    if current_channel_id:
        query['channel_id'] = current_channel_id

    # Get total count with optimized aggregation
    total = videos_collection.count_documents(query)

    # Enhanced video retrieval with additional fields
    # Build sort criteria
    sort_criteria = []
    if sort_column:
        if sort_column == 'views':
            sort_field = 'view_count'
        elif sort_column == 'retention':
            sort_field = 'retention_30s'
        elif sort_column == 'published_at':
            sort_field = 'published_at'
        elif sort_column == 'duration':
            sort_field = 'duration_seconds'
        elif sort_column == 'likes':
            sort_field = 'like_count'
        else:
            sort_field = 'published_at'  # Default fallback
        
        sort_direction = -1 if sort_order == 'desc' else 1
        sort_criteria.append((sort_field, sort_direction))
    else:
        # Default sorting
        sort_criteria.append(('published_at', -1))
    
    videos = list(videos_collection.find(query, {
        'title': 1, 'video_id': 1, 'published_at': 1, 'thumbnail_url': 1,
        'duration_seconds': 1, 'view_count': 1, 'like_count': 1,
        'average_view_percentage': 1, 'is_compilation': 1, 'actor': 1,
        'description': 1, 'tags': 1, 'retention_30s': 1,
        'user_compilation_usage': 1, 'compilation_usage_stats': 1
    })
    .sort(sort_criteria)
    .skip((page - 1) * per_page)
    .limit(per_page))

    # Calculate enhanced pagination info
    total_pages = (total + per_page - 1) // per_page
    has_prev = page > 1
    has_next = page < total_pages

    # Get quick statistics for dashboard (filtered by channel)
    channel_filter = {'channel_id': current_channel_id} if current_channel_id else {}
    quick_stats = {
        'total_videos': total,
        'compilations': videos_collection.count_documents({'is_compilation': True, **channel_filter}),
        'high_retention': videos_collection.count_documents({'retention_30s': {'$gte': 70}, **channel_filter}),
        'user_compilations': user_compilations_collection.count_documents({**channel_filter})
    }

    # Get available tags for filter dropdown
    available_tags = get_available_tags()

    return render_template('index.html',
                           videos=videos,
                           page=page,
                           total_pages=total_pages,
                           has_prev=has_prev,
                           has_next=has_next,
                           search_query=search_query,
                           actor_filter=actor_filter,
                           compilation_filter=compilation_filter,
                           retention_filter=retention_filter,
                           tag_filter=tag_filter,
                           sort_column=sort_column,
                           sort_order=sort_order,
                           available_tags=available_tags,
                           quick_stats=quick_stats,
                           total=total,
                           now=datetime.now())


def get_available_tags():
    """Get all available tags for filter dropdown"""
    pipeline = [
        {'$unwind': '$tags'},
        {'$group': {'_id': '$tags', 'count': {'$sum': 1}}},
        {'$sort': {'count': -1, '_id': 1}}
    ]
    
    results = list(videos_collection.aggregate(pipeline))
    return [result['_id'] for result in results]


@app.route('/video_detail/<video_id>')
@login_required
def video_detail(video_id):
    try:
        # Get video from database
        video = videos_collection.find_one({'video_id': video_id}, {
            'title': 1, 'video_id': 1, 'published_at': 1, 'thumbnail_url': 1,
            'duration_seconds': 1, 'view_count': 1, 'like_count': 1, 'comment_count': 1,
            'description': 1, 'average_view_percentage': 1, 'retention_30s': 1,
            'estimated_minutes_watched': 1, 'average_view_duration': 1, 'duration': 1,
            'actor': 1, 'tags': 1, 'is_compilation': 1, 'compilation_usage_stats': 1,
            'user_compilation_usage': 1, 'created_at': 1, 'updated_at': 1
        })

        if not video:
            return "Video not found", 404

        # Get compilation data if this video IS a compilation
        compilation_data = None
        if video.get('is_compilation'):
            compilation_data = compilations_collection.find_one(
                {'video_id': video_id})

        # Get compilations that include this video
        auto_compilations = list(compilations_collection.find({
            'timestamps.video_id': video_id
        }))

        user_compilations = list(user_compilations_collection.find({
            'timestamps.video_id': video_id
        }))

        # Get detailed info for each compilation
        auto_compilation_details = []
        for comp in auto_compilations:
            # Find this video's position in the compilation
            position = 1
            first_video = False
            for i, ts in enumerate(comp.get('timestamps', [])):
                if ts.get('video_id') == video_id:
                    position = i + 1
                    first_video = (i == 0)
                    break

            auto_compilation_details.append({
                'title': comp.get('title', 'Untitled'),
                'video_id': comp.get('video_id', ''),
                'duration_rounded': comp.get('duration_rounded', 0),
                'position': position,
                'is_first_video': first_video,
                'created_at': comp.get('created_at')
            })

        user_compilation_details = []
        for comp in user_compilations:
            # Find this video's position in the compilation
            position = 1
            first_video = False
            for i, ts in enumerate(comp.get('timestamps', [])):
                if ts.get('video_id') == video_id:
                    position = i + 1
                    first_video = (i == 0)
                    break

            user_compilation_details.append({
                'title': comp.get('title', 'Untitled'),
                'video_id': comp.get('video_id', ''),
                'duration_rounded': comp.get('duration_rounded', 0),
                'position': position,
                'is_first_video': first_video,
                'created_at': comp.get('created_at')
            })

        # Convert ObjectId to string for template
        if video.get('_id'):
            video['_id'] = str(video['_id'])

        return render_template('video_detail.html',
                               video=video,
                               compilation_data=compilation_data,
                               auto_compilations=auto_compilation_details,
                               user_compilations=user_compilation_details,
                               now=datetime.now())

    except Exception as e:
        return f"Error loading video details: {str(e)}", 500


@app.route('/compilation_detail/<video_id>')
@login_required
def compilation_detail(video_id):
    try:
        # Get compilation from database
        compilation = compilations_collection.find_one({'video_id': video_id})

        if not compilation:
            return "Compilation not found", 404

        # Get the original video that contains this compilation
        original_video = videos_collection.find_one({'video_id': video_id})

        # Get detailed info for each segment video
        segment_videos = {}
        segment_analysis = {
            'quality_distribution': {'high': 0, 'medium': 0, 'low': 0},
            'avg_retention': 0,
            'total_original_views': 0,
            'quality_score': 0
        }

        if compilation.get('timestamps'):
            retention_rates = []
            total_views = 0

            # Debug
            print(f"Processing {len(compilation['timestamps'])} timestamps")

            for timestamp in compilation['timestamps']:
                if timestamp.get('title'):
                    # Debug
                    print(f"Looking for video: {timestamp['title']}")

                    # Try exact match first
                    segment_video = videos_collection.find_one({
                        'title': timestamp['title']
                    })

                    # If still not found, try different search strategies
                    if not segment_video:
                        # Escape special regex characters
                        escaped_title = re.escape(timestamp['title'])

                        # Try regex search with escaped title
                        segment_video = videos_collection.find_one({
                            '$and': [
                                {'title': {'$regex': escaped_title, '$options': 'i'}},
                                {
                                    '$or': [
                                        {'is_compilation': {'$exists': False}},
                                        {'is_compilation': False}
                                    ]
                                }
                            ]
                        })

                        # If still not found, try partial match
                        if not segment_video:
                            # Split title into words and search for any containing these words
                            title_words = timestamp['title'].split()
                            if title_words:
                                word_regexes = [re.escape(word) for word in title_words if len(
                                    word) > 2]  # Skip very short words
                                if word_regexes:
                                    segment_video = videos_collection.find_one({
                                        '$and': [
                                            {
                                                '$or': [
                                                    {'title': {'$regex': '|'.join(
                                                        word_regexes), '$options': 'i'}},
                                                    {'description': {'$regex': '|'.join(
                                                        word_regexes), '$options': 'i'}}
                                                ]
                                            },
                                            {
                                                '$or': [
                                                    {'is_compilation': {
                                                        '$exists': False}},
                                                    {'is_compilation': False}
                                                ]
                                            }
                                        ]
                                    })

                        # If still not found, try without compilation filter
                        if not segment_video:
                            segment_video = videos_collection.find_one({
                                'title': {'$regex': escaped_title, '$options': 'i'}
                            })

                    if segment_video:
                        segment_videos[segment_video['video_id']
                                       ] = segment_video
                        # Debug
                        print(f"Found video: {segment_video['title']}")

                        # Analyze quality
                        retention = segment_video.get('retention_30s', 0)
                        retention_rates.append(retention)
                        total_views += segment_video.get('view_count', 0)

                        if retention >= 70:
                            segment_analysis['quality_distribution']['high'] += 1
                        elif retention >= 50:
                            segment_analysis['quality_distribution']['medium'] += 1
                        else:
                            segment_analysis['quality_distribution']['low'] += 1
                    else:
                        # Debug - show what we're looking for vs what's available
                        print(f"Video not found: {timestamp['title']}")

                        # Additional debugging: show similar titles
                        similar_videos = videos_collection.find({
                            'title': {'$regex': re.escape(timestamp['title'][:10]), '$options': 'i'}
                        }).limit(3)

                        print("Similar titles found:")
                        for similar in similar_videos:
                            print(f"  - {similar.get('title', 'No title')}")

            # Calculate averages
            if retention_rates:
                segment_analysis['avg_retention'] = sum(
                    retention_rates) / len(retention_rates)
                segment_analysis['total_original_views'] = total_views

                # Calculate quality score (1-10)
                high_ratio = segment_analysis['quality_distribution']['high'] / len(
                    retention_rates)
                segment_analysis['quality_score'] = min(10, max(1,
                                                                (segment_analysis['avg_retention'] / 10) + (
                                                                    high_ratio * 5)
                                                                ))

        # Debug: Print what we found
        print(
            f"Found {len(segment_videos)} segment videos out of {len(compilation.get('timestamps', []))} timestamps")

        # Convert ObjectId to string
        if compilation.get('_id'):
            compilation['_id'] = str(compilation['_id'])

        return render_template('compilation_detail.html',
                               compilation=compilation,
                               original_video=original_video,
                               segment_videos=segment_videos,
                               segment_analysis=segment_analysis,
                               now=datetime.now())

    except Exception as e:
        print(f"Error in compilation_detail: {str(e)}")  # Debug
        return f"Error loading compilation details: {str(e)}", 500


# @app.route('/import')
# def import_page():
#     return render_template('import.html')


@app.route('/import', methods=['GET', 'POST'])
@login_required
def import_data():
    """Import videos from JSON file"""
    if request.method == 'POST':
        if 'file' not in request.files:
            return jsonify({'error': 'No file selected'}), 400

        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400

        if file and file.filename.endswith('.json'):
            # Save uploaded file temporarily
            temp_path = 'temp_upload.json'
            file.save(temp_path)

            try:
                result = VideoManager.import_from_json(temp_path)
                if len(result) == 3:  # Error case
                    imported, total, error = result
                    return jsonify({'error': f'Import failed: {error}'}), 500
                else:
                    imported, total = result
                    os.remove(temp_path)  # Clean up
                    return jsonify({
                        'success': True,
                        'imported': imported,
                        'total': total,
                        'message': f'Successfully imported {imported} out of {total} videos'
                    })
            except Exception as e:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
                return jsonify({'error': str(e)}), 500
        else:
            return jsonify({'error': 'Please upload a JSON file'}), 400

    return render_template('import.html')


@app.route('/import-videos', methods=['POST'])
@login_required
def import_videos():
    """Enhanced import endpoint that matches the frontend expectations"""
    try:
        if 'json_file' not in request.files:
            return jsonify({'success': False, 'error': 'No file selected'}), 400

        file = request.files['json_file']
        if file.filename == '':
            return jsonify({'success': False, 'error': 'No file selected'}), 400

        if not (file and file.filename.endswith('.json')):
            return jsonify({'success': False, 'error': 'Please upload a JSON file'}), 400

        # Get additional options from form
        # Default to updating existing videos for comprehensive data refresh
        skip_existing = request.form.get(
            'skip_existing', 'false').lower() == 'true'
        update_existing = request.form.get(
            'update_existing', 'true').lower() == 'true'
        validate_data = request.form.get(
            'validate_data', 'true').lower() == 'true'

        # Save uploaded file temporarily
        temp_path = f'temp_upload_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
        file.save(temp_path)

        try:
            # Use the enhanced import method
            result = VideoManager.enhanced_import_from_json(
                temp_path,
                skip_existing=skip_existing,
                update_existing=update_existing,
                validate_data=validate_data
            )

            # Clean up temp file
            if os.path.exists(temp_path):
                os.remove(temp_path)

            if 'error' in result:
                return jsonify({
                    'success': False,
                    'error': result['error']
                }), 500
            else:
                # Create detailed success message
                imported = result.get('imported', 0)
                updated = result.get('updated', 0)
                total = result.get('total', 0)
                skipped = result.get('skipped', 0)

                # Build comprehensive message
                parts = []
                if imported > 0:
                    if updated > 0:
                        parts.append(f"Updated {updated} existing videos")
                    new_videos = imported - updated
                    if new_videos > 0:
                        parts.append(f"Added {new_videos} new videos")
                if skipped > 0:
                    parts.append(f"Skipped {skipped} videos")
                deleted_count = result.get('deleted_count', 0)
                if deleted_count > 0:
                    parts.append(f"Marked {deleted_count} videos as deleted")

                message = f"Successfully processed {total} videos. " + ". ".join(parts) + "."

                # Include processing results if available
                processing_info = ""
                if 'processing_results' in result:
                    pr = result['processing_results']
                    if pr.get('processed', 0) > 0:
                        processing_info = f" Processed {pr['processed']} videos across {pr.get('new_compilations', 0)} new and {pr.get('updated_compilations', 0)} updated compilations."

                return jsonify({
                    'success': True,
                    'imported_count': imported,
                    'updated_count': updated,
                    'total_count': total,
                    'skipped_count': skipped,
                    'deleted_count': result.get('deleted_count', 0),
                    'processing_results': result.get('processing_results', {}),
                    'stats_result': result.get('stats_result', {}),
                    'errors': result.get('errors', []),
                    'message': message + processing_info,
                    'detailed_results': {
                        'new_videos': imported - updated,
                        'updated_videos': updated,
                        'skipped_videos': skipped,
                        'deleted_videos': result.get('deleted_count', 0),
                        'compilation_updates': result.get('processing_results', {}).get('updated_compilations', 0)
                    }
                })

        except Exception as e:
            if os.path.exists(temp_path):
                os.remove(temp_path)
            return jsonify({
                'success': False,
                'error': f'Import processing failed: {str(e)}'
            }), 500

    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'File upload failed: {str(e)}'
        }), 500
















# ==================== NEW COMPILATION CREATION ROUTES ====================

@app.route('/create-compilation')
@login_required
def create_compilation():
    """Advanced compilation creation interface with step-by-step wizard"""
    return render_template('create_compilation.html')


@app.route('/api/available-durations')
@login_required
def api_available_durations():
    """API endpoint to get available compilation durations with analytics"""
    try:
        durations = compilation_creator.get_available_durations()
        
        # Add popularity metrics for each duration
        duration_analytics = []
        for duration in durations:
            existing_count = compilations_collection.count_documents({
                'duration_rounded': duration
            })
            user_count = user_compilations_collection.count_documents({
                'duration_rounded': duration
            })
            
            duration_analytics.append({
                'duration': duration,
                'existing_compilations': existing_count,
                'user_compilations': user_count,
                'total': existing_count + user_count,
                'popularity_score': existing_count * 2 + user_count  # Weighted popularity
            })
        
        # Sort by popularity for better UX
        duration_analytics.sort(key=lambda x: x['popularity_score'], reverse=True)
        
        return jsonify({
            'success': True,
            'durations': [d['duration'] for d in duration_analytics],
            'analytics': duration_analytics
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'durations': [5, 10, 15, 20, 25, 30]  # Fallback durations
        })


class CompilationCreator:
    """
    Advanced compilation creation system that intelligently selects videos
    based on multiple criteria including retention rates, usage frequency,
    publication dates, and duration constraints.
    """

    def __init__(self, videos_collection, compilations_collection, user_compilations_collection):
        self.videos_collection = videos_collection
        self.compilations_collection = compilations_collection
        self.user_compilations_collection = user_compilations_collection

        # Configuration constants for compilation creation logic
        # Maximum times a video can be used in compilations per year
        self.MAX_ANNUAL_USAGE = 10
        self.RETENTION_WEIGHT = 0.6  # Weight factor for retention rate in scoring algorithm
        self.VIEW_COUNT_WEIGHT = 0.3  # Weight factor for view count in scoring algorithm
        self.FRESHNESS_WEIGHT = 0.1  # Weight factor for video freshness in scoring algorithm

    def get_available_durations(self) -> List[int]:
        """
        Retrieve all available compilation durations based on existing compilations
        in the database, rounded to nearest 5-minute intervals.
        
        Returns:
            List[int]: Sorted list of available duration options (5, 10, 15, 20, etc.)
        """
        pipeline = [
            {'$match': {'duration_rounded': {'$exists': True}}},
            {'$group': {'_id': '$duration_rounded'}},
            {'$sort': {'_id': 1}}
        ]

        existing_durations = list(
            self.compilations_collection.aggregate(pipeline))
        durations = [dur['_id'] for dur in existing_durations]

        # Add common duration options if not present
        standard_durations = [5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60]
        for duration in standard_durations:
            if duration not in durations:
                durations.append(duration)

        return sorted(durations)

    def categorize_videos_by_retention(self, videos: List[Dict], from_date: Optional[str] = None) -> Dict[VideoCategory, List[Dict]]:
        """
        Categorize videos into retention rate percentiles with sophisticated filtering
        and sorting algorithms to ensure optimal video selection.
        
        Args:
            videos: List of video documents from database
            from_date: Optional date filter in 'YYYY-MM-DD' format
            
        Returns:
            Dict mapping video categories to sorted lists of videos
        """
        # Filter videos based on criteria
        filtered_videos = []
        current_date = datetime.utcnow()
        one_year_ago = current_date - timedelta(days=365)

        for video in videos:
            # Skip compilation videos
            if video.get('is_compilation', False):
                continue

            # Apply date filter if specified
            if from_date:
                try:
                    video_date = datetime.strptime(
                        video.get('published_at', ''), '%Y-%m-%d')
                    filter_date = datetime.strptime(from_date, '%Y-%m-%d')
                    if video_date < filter_date:
                        continue
                except (ValueError, TypeError):
                    # Skip videos with invalid dates
                    continue

            # Check annual usage limit
            usage_stats = video.get('compilation_usage_stats', {})
            total_usage = usage_stats.get('total_inclusions', 0)
            if total_usage >= self.MAX_ANNUAL_USAGE:
                continue

            # Ensure video has required metrics
            if not all(key in video for key in ['average_view_percentage', 'view_count']):
                continue

            filtered_videos.append(video)

        if not filtered_videos:
            return {category: [] for category in VideoCategory}

        # Sort videos by comprehensive scoring algorithm
        def calculate_video_score(video: Dict) -> float:
            """
            Calculate composite score for video selection using multiple weighted factors
            """
            retention_rate = video.get('average_view_percentage', 0) / 100.0
            view_count = video.get('view_count', 0)

            # Normalize view count (log scale to prevent outliers from dominating)
            max_views = max(v.get('view_count', 1) for v in filtered_videos)
            normalized_views = math.log(
                view_count + 1) / math.log(max_views + 1)

            # Calculate freshness factor (newer videos get slight boost)
            try:
                video_date = datetime.strptime(
                    video.get('published_at', ''), '%Y-%m-%d')
                days_old = (current_date - video_date).days
                # Decreases linearly over a year
                freshness_factor = max(0, 1 - (days_old / 365))
            except (ValueError, TypeError):
                freshness_factor = 0

            # Composite score calculation
            score = (
                self.RETENTION_WEIGHT * retention_rate +
                self.VIEW_COUNT_WEIGHT * normalized_views +
                self.FRESHNESS_WEIGHT * freshness_factor
            )

            return score

        # Sort videos by calculated scores
        sorted_videos = sorted(
            filtered_videos, key=calculate_video_score, reverse=True)

        # Divide into quartiles (25% each)
        total_count = len(sorted_videos)
        quartile_size = total_count // 4

        categories = {
            VideoCategory.TOP_25_PERCENT: sorted_videos[:quartile_size],
            VideoCategory.SECOND_25_PERCENT: sorted_videos[quartile_size:2*quartile_size],
            VideoCategory.THIRD_25_PERCENT: sorted_videos[2*quartile_size:3*quartile_size],
            VideoCategory.BOTTOM_25_PERCENT: sorted_videos[3*quartile_size:]
        }

        return categories

    def select_first_video(self, duration_rounded: int, categorized_videos: Dict[VideoCategory, List[Dict]]) -> Optional[Dict]:
        """
        Select the optimal first video for a compilation using advanced selection criteria.
        The first video is crucial as it determines viewer engagement and retention.
        
        Args:
            duration_rounded: Target compilation duration
            categorized_videos: Videos categorized by retention percentiles
            
        Returns:
            Selected video document or None if no suitable video found
        """
        # Start with top 25% videos (highest retention rates)
        candidates = categorized_videos.get(VideoCategory.TOP_25_PERCENT, [])

        if not candidates:
            # Fallback to second tier if top tier is empty
            candidates = categorized_videos.get(
                VideoCategory.SECOND_25_PERCENT, [])

        # Filter out videos already used as first video in same duration category
        for video in candidates:
            usage_stats = video.get('compilation_usage_stats', {})
            first_video_by_duration = usage_stats.get(
                'first_video_by_duration', {})
            duration_key = f"{duration_rounded}min"

            # Check if this video was already used as first video in this duration category
            if first_video_by_duration.get(duration_key, 0) == 0:
                return video

        # If no unused first videos found, return None
        return None

    def calculate_compilation_duration_seconds(self, selected_videos: List[Dict]) -> int:
        """
        Calculate the total duration in seconds for selected videos,
        accounting for potential transitions and intro/outro segments.
        
        Args:
            selected_videos: List of selected video documents
            
        Returns:
            Total duration in seconds
        """
        total_duration = 0
        transition_time = 2  # 2 seconds transition between videos

        for video in selected_videos:
            video_duration = video.get('duration_seconds', 0)
            total_duration += video_duration

        # Add transition times (n-1 transitions for n videos)
        if len(selected_videos) > 1:
            total_duration += (len(selected_videos) - 1) * transition_time

        return total_duration

    def select_additional_videos(self, target_duration_minutes: int, first_video: Dict,
                                 categorized_videos: Dict[VideoCategory, List[Dict]]) -> List[Dict]:
        """
        Intelligently select additional videos to fill the compilation duration
        using a sophisticated algorithm that balances content variety and engagement.
        
        Args:
            target_duration_minutes: Target compilation duration in minutes
            first_video: Already selected first video
            categorized_videos: Videos categorized by retention percentiles
            
        Returns:
            List of selected videos including the first video
        """
        selected_videos = [first_video]
        target_duration_seconds = target_duration_minutes * 60
        current_duration = first_video.get('duration_seconds', 0)

        # Create a pool of remaining candidates from all categories
        all_candidates = []
        used_video_ids = {first_video['video_id']}

        # Prioritize higher quality videos but include variety
        category_weights = {
            VideoCategory.TOP_25_PERCENT: 0.4,      # 40% from top tier
            VideoCategory.SECOND_25_PERCENT: 0.35,  # 35% from second tier
            VideoCategory.THIRD_25_PERCENT: 0.20,   # 20% from third tier
            VideoCategory.BOTTOM_25_PERCENT: 0.05   # 5% from bottom tier
        }

        # Build weighted candidate pool
        for category, weight in category_weights.items():
            category_videos = categorized_videos.get(category, [])
            # Filter out already used videos
            available_videos = [
                v for v in category_videos if v['video_id'] not in used_video_ids]

            # Add videos with category weight information
            for video in available_videos:
                video_copy = video.copy()
                video_copy['_selection_weight'] = weight
                all_candidates.append(video_copy)

        # Sort candidates by a combination of duration fit and quality score
        def selection_score(video: Dict) -> float:
            """Calculate selection score for remaining video selection"""
            remaining_duration = target_duration_seconds - current_duration
            video_duration = video.get('duration_seconds', 0)

            # Duration fit score (prefer videos that fit well in remaining time)
            if video_duration <= remaining_duration:
                duration_fit = 1.0 - \
                    abs(remaining_duration - video_duration) / \
                    remaining_duration
            else:
                # Penalty for videos that are too long
                duration_fit = max(
                    0, 1.0 - (video_duration - remaining_duration) / remaining_duration)

            # Quality score based on retention and category
            quality_score = (video.get('average_view_percentage', 0) /
                             100.0) * video.get('_selection_weight', 0.1)

            # Combined score
            return 0.7 * duration_fit + 0.3 * quality_score

        # Continue selecting videos until target duration is approximately met
        while current_duration < target_duration_seconds * 0.95:  # 95% of target duration
            if not all_candidates:
                break

            # Re-sort candidates based on current state
            available_candidates = [
                v for v in all_candidates
                if v['video_id'] not in used_video_ids and
                v.get('duration_seconds', 0) <= (
                    target_duration_seconds - current_duration) + 30  # 30s buffer
            ]

            if not available_candidates:
                break

            # Select best fitting video
            available_candidates.sort(key=selection_score, reverse=True)
            selected_video = available_candidates[0]

            selected_videos.append(selected_video)
            used_video_ids.add(selected_video['video_id'])
            current_duration += selected_video.get('duration_seconds', 0)

            # Remove selected video from future consideration
            all_candidates = [
                v for v in all_candidates if v['video_id'] != selected_video['video_id']]

        return selected_videos

    def create_compilation(self, duration_minutes: int, from_date: Optional[str] = None,
                           title_prefix: str = "Auto-Generated", user_id: str = "system") -> Dict:
        """
        Create a new compilation with sophisticated video selection and metadata generation.
        This is the main entry point for compilation creation.
        
        Args:
            duration_minutes: Target duration in minutes (will be rounded to nearest 5)
            from_date: Optional date filter for video selection
            title_prefix: Prefix for the generated compilation title
            user_id: ID of the user creating the compilation
            
        Returns:
            Dictionary containing creation results and compilation metadata
        """
        # Round duration to nearest 5 minutes
        duration_rounded = round(duration_minutes / 5) * 5
        if duration_rounded < 5:
            duration_rounded = 5

        # Get all videos for categorization
        all_videos = list(self.videos_collection.find({}))

        if not all_videos:
            return {
                'success': False,
                'error': 'No videos available in database',
                'compilation_id': None
            }

        # Categorize videos by retention rate with date filtering
        categorized_videos = self.categorize_videos_by_retention(
            all_videos, from_date)

        # Check if any videos are available after filtering
        total_available = sum(len(videos)
                              for videos in categorized_videos.values())
        if total_available == 0:
            return {
                'success': False,
                'error': 'No videos available after applying filters',
                'compilation_id': None
            }

        # Select the first video (highest priority)
        first_video = self.select_first_video(
            duration_rounded, categorized_videos)
        if not first_video:
            return {
                'success': False,
                'error': 'No suitable first video found',
                'compilation_id': None
            }

        # Select additional videos to fill the compilation
        selected_videos = self.select_additional_videos(
            duration_rounded, first_video, categorized_videos)

        if len(selected_videos) < 2:
            return {
                'success': False,
                'error': 'Insufficient videos to create meaningful compilation',
                'compilation_id': None
            }

        # Calculate actual total duration
        actual_duration_seconds = self.calculate_compilation_duration_seconds(
            selected_videos)

        # Generate compilation metadata
        compilation_doc = self._generate_compilation_document(
            selected_videos, duration_rounded, actual_duration_seconds,
            title_prefix, user_id, from_date
        )

        # Save to database
        try:
            result = self.user_compilations_collection.insert_one(
                compilation_doc)
            compilation_id = str(result.inserted_id)

            return {
                'success': True,
                'compilation_id': compilation_id,
                'selected_videos_count': len(selected_videos),
                'target_duration_minutes': duration_rounded,
                'actual_duration_seconds': actual_duration_seconds,
                'actual_duration_minutes': round(actual_duration_seconds / 60, 1),
                'video_categories_used': self._analyze_category_usage(selected_videos, categorized_videos)
            }

        except Exception as e:
            return {
                'success': False,
                'error': f'Database error: {str(e)}',
                'compilation_id': None
            }

    def _generate_compilation_document(self, selected_videos: List[Dict], duration_rounded: int,
                                       actual_duration_seconds: int, title_prefix: str,
                                       user_id: str, from_date: Optional[str]) -> Dict:
        """
        Generate comprehensive compilation document with all necessary metadata
        """
        # Create timestamps for the compilation
        timestamps = []
        current_time_seconds = 0

        for video in selected_videos:
            # Format timestamp as MM:SS or H:MM:SS
            minutes = current_time_seconds // 60
            seconds = current_time_seconds % 60

            if minutes >= 60:
                hours = minutes // 60
                minutes = minutes % 60
                timestamp = f"{hours}:{minutes:02d}:{seconds:02d}"
            else:
                timestamp = f"{minutes}:{seconds:02d}"

            timestamps.append({
                'timestamp': timestamp,
                'title': video['title'],
                'video_id': video['video_id'],
                'original_duration': video.get('duration_seconds', 0),
                'retention_rate': video.get('average_view_percentage', 0)
            })

            # 2s transition
            current_time_seconds += video.get('duration_seconds', 0) + 2

        # Generate intelligent title
        compilation_title = self._generate_compilation_title(
            selected_videos, title_prefix, duration_rounded)

        # Calculate compilation metrics
        avg_retention = sum(v.get('average_view_percentage', 0)
                            for v in selected_videos) / len(selected_videos)
        total_original_views = sum(v.get('view_count', 0)
                                   for v in selected_videos)

        compilation_doc = {
            'title': compilation_title,
            'duration_rounded': duration_rounded,
            'actual_duration_seconds': actual_duration_seconds,
            'status': CompilationStatus.NOT_PUBLISHED.value,
            'created_by': user_id,
            'created_at': datetime.utcnow(),
            'updated_at': datetime.utcnow(),
            'from_date_filter': from_date,
            'video_count': len(selected_videos),
            'timestamps': timestamps,
            'metadata': {
                'average_retention_rate': round(avg_retention, 2),
                'total_original_views': total_original_views,
                'creation_algorithm_version': '1.0',
                'selection_criteria': {
                    'max_annual_usage': self.MAX_ANNUAL_USAGE,
                    'retention_weight': self.RETENTION_WEIGHT,
                    'view_count_weight': self.VIEW_COUNT_WEIGHT,
                    'freshness_weight': self.FRESHNESS_WEIGHT
                }
            },
            'export_data': {
                'last_exported': None,
                'export_count': 0
            }
        }

        return compilation_doc

    def _generate_compilation_title(self, videos: List[Dict], prefix: str, duration: int) -> str:
        """Generate intelligent compilation title based on content analysis"""
        # Analyze common themes in video titles
        all_words = []
        for video in videos:
            title_words = video.get('title', '').lower().split()
            all_words.extend(title_words)

        # Find most common meaningful words (exclude common words)
        common_stopwords = {'the', 'a', 'an', 'and', 'or', 'but',
                            'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by'}
        meaningful_words = [word for word in all_words if len(
            word) > 2 and word not in common_stopwords]

        word_counts = {}
        for word in meaningful_words:
            word_counts[word] = word_counts.get(word, 0) + 1

        # Get top themes
        top_words = sorted(word_counts.items(),
                           key=lambda x: x[1], reverse=True)[:3]

        if top_words:
            theme = ' '.join([word[0].capitalize() for word in top_words])
            return f"{prefix} - {theme} Collection ({duration} min)"
        else:
            return f"{prefix} - Compilation ({duration} min) - {datetime.now().strftime('%Y%m%d')}"

    def _analyze_category_usage(self, selected_videos: List[Dict],
                                categorized_videos: Dict[VideoCategory, List[Dict]]) -> Dict:
        """Analyze which video categories were used in the final selection"""
        category_usage = {category.value: 0 for category in VideoCategory}

        # Create reverse lookup for video categories
        video_to_category = {}
        for category, videos in categorized_videos.items():
            for video in videos:
                video_to_category[video['video_id']] = category

        # Count usage by category
        for video in selected_videos:
            category = video_to_category.get(video['video_id'])
            if category:
                category_usage[category.value] += 1

        return category_usage

    def get_compilation_preview(self, compilation_id: str) -> Optional[Dict]:
        """Get detailed preview of a created compilation"""
        try:
            compilation = self.user_compilations_collection.find_one({
                '_id': ObjectId(compilation_id)
            })

            if not compilation:
                return None

            # Enrich with additional video details
            enriched_timestamps = []
            for timestamp_entry in compilation.get('timestamps', []):
                video_detail = self.videos_collection.find_one({
                    'video_id': timestamp_entry['video_id']
                })

                if video_detail:
                    enriched_entry = timestamp_entry.copy()
                    enriched_entry['video_details'] = {
                        'view_count': video_detail.get('view_count', 0),
                        'like_count': video_detail.get('like_count', 0),
                        'published_at': video_detail.get('published_at', ''),
                        'thumbnail_url': video_detail.get('thumbnail_url', ''),
                        'full_description': video_detail.get('description', '')[:200] + '...'
                    }
                    enriched_timestamps.append(enriched_entry)

            compilation['enriched_timestamps'] = enriched_timestamps
            return compilation

        except Exception as e:
            return None

    def update_compilation_status(self, compilation_id: str, new_status: CompilationStatus) -> bool:
        """Update the publication status of a compilation"""
        try:
            result = self.user_compilations_collection.update_one(
                {'_id': ObjectId(compilation_id)},
                {
                    '$set': {
                        'status': new_status.value,
                        'updated_at': datetime.utcnow(),
                        'published_at': datetime.utcnow() if new_status == CompilationStatus.PUBLISHED else None
                    }
                }
            )
            return result.modified_count > 0
        except Exception as e:
            return False

    def delete_compilation(self, compilation_id: str) -> bool:
        """Delete a compilation (only if not published)"""
        try:
            compilation = self.user_compilations_collection.find_one({
                '_id': ObjectId(compilation_id)
            })

            if not compilation:
                return False

            # Only allow deletion of non-published compilations
            if compilation.get('status') == CompilationStatus.PUBLISHED.value:
                return False

            result = self.user_compilations_collection.delete_one({
                '_id': ObjectId(compilation_id)
            })

            return result.deleted_count > 0

        except Exception as e:
            return False

    def get_user_compilations(self, user_id: str = "system", status_filter: Optional[str] = None) -> List[Dict]:
        """Get all compilations created by a specific user with optional status filtering"""
        query = {'created_by': user_id}

        if status_filter:
            query['status'] = status_filter

        compilations = list(self.user_compilations_collection.find(query)
                            .sort('created_at', -1))

        # Convert ObjectId to string for JSON serialization
        for comp in compilations:
            comp['_id'] = str(comp['_id'])

        return compilations


@app.route('/api/create-compilation', methods=['POST'])
@login_required
def api_create_compilation():
    """Advanced API endpoint for creating new compilations with comprehensive validation"""
    try:
        data = request.get_json()
        duration = data.get('duration', 10)
        compilation_type = data.get('compilation_type', 'default')
        from_date = data.get('from_date')
        to_date = data.get('to_date')
        tags = data.get('tags', [])  # Get tags filter from request
        user_id = data.get('user_id', 'system')  # In production, get from session

        # Validate input parameters
        if not isinstance(duration, int) or duration < 5 or duration > 120:
            return jsonify({
                'success': False,
                'error': 'Duration must be between 5 and 120 minutes'
            })

        # Validate date formats if provided
        from datetime import datetime
        if from_date:
            try:
                datetime.strptime(from_date, '%Y-%m-%d')
            except ValueError:
                return jsonify({
                    'success': False,
                    'error': 'Invalid from_date format. Use YYYY-MM-DD'
                })

        if to_date:
            try:
                datetime.strptime(to_date, '%Y-%m-%d')
            except ValueError:
                return jsonify({
                    'success': False,
                    'error': 'Invalid to_date format. Use YYYY-MM-DD'
                })

        # Validate tags parameter
        if tags and not isinstance(tags, list):
            return jsonify({
                'success': False,
                'error': 'Tags must be a list'
            })

        # Get current channel info for the compilation
        current_channel_id = get_current_channel_id()
        current_channel_name = None
        if current_channel_id:
            channel_info = ChannelManager.get_channel_by_id(current_channel_id)
            if channel_info:
                current_channel_name = channel_info.get('channel_name')
        
        # Create compilation using advanced algorithm
        result = compilation_creator.create_compilation(
            duration_minutes=duration,
            from_date=from_date,
            to_date=to_date,
            tags=tags,  # Pass tags filter to compilation creator
            title_prefix='Auto-Generated',  # Fixed title prefix as requested
            user_id=user_id,
            compilation_type=compilation_type,
            channel_id=current_channel_id,
            channel_name=current_channel_name
        )
        
        if result['success']:
            # Update usage statistics for selected videos
            compilation = user_compilations_collection.find_one({
                '_id': ObjectId(result['compilation_id'])
            })
            
            if compilation:
                # Update video usage tracking
                for timestamp_entry in compilation.get('timestamps', []):
                    video_id = timestamp_entry.get('video_id')
                    videos_collection.update_one(
                        {'video_id': video_id},
                        {
                            '$inc': {
                                'user_compilation_usage.total_inclusions': 1,
                                f'user_compilation_usage.usage_by_duration.{duration}min': 1
                            },
                            '$set': {
                                'user_compilation_usage.last_used': datetime.utcnow()
                            }
                        }
                    )
                
                # Update first video usage specifically
                first_video_id = compilation['timestamps'][0]['video_id'] if compilation['timestamps'] else None
                if first_video_id:
                    videos_collection.update_one(
                        {'video_id': first_video_id},
                        {
                            '$inc': {
                                'user_compilation_usage.first_video_count': 1
                            }
                        }
                    )
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Compilation creation failed: {str(e)}'
        })


@app.route('/user-compilations')
@login_required
def user_compilations():
    """Enhanced user compilations dashboard with comprehensive management features"""
    page = request.args.get('page', 1, type=int)
    status_filter = request.args.get('status')
    user_id = request.args.get('user_id', 'system')  # In production, get from session
    per_page = 10
    
    # Build query with enhanced filtering
    query = {'created_by': user_id}
    if status_filter:
        query['status'] = status_filter
    
    # Channel filtering - only show compilations from current channel
    current_channel_id = get_current_channel_id()
    if current_channel_id:
        query['channel_id'] = current_channel_id
    
    # Get paginated results with optimized fields
    total = user_compilations_collection.count_documents(query)
    compilations = list(user_compilations_collection.find(query)
                       .sort('created_at', -1)
                       .skip((page - 1) * per_page)
                       .limit(per_page))
    
    # Convert ObjectId for template rendering
    for comp in compilations:
        comp['_id'] = str(comp['_id'])
    
    # Calculate comprehensive statistics (filtered by channel)
    channel_filter = {'channel_id': current_channel_id} if current_channel_id else {}
    base_filter = {'created_by': user_id, **channel_filter}
    
    stats = {
        'total': user_compilations_collection.count_documents(base_filter),
        'generated': user_compilations_collection.count_documents({
            **base_filter,
            'status': CompilationStatus.GENERATED.value
        }),
        'to_do': user_compilations_collection.count_documents({
            **base_filter,
            'status': CompilationStatus.TO_DO.value
        }),
        'ready': user_compilations_collection.count_documents({
            **base_filter,
            'status': CompilationStatus.READY.value
        }),
        'uploaded': user_compilations_collection.count_documents({
            **base_filter,
            'status': CompilationStatus.UPLOADED.value
        }),
        'total_videos': 0,
        'total_duration': 0
    }
    
    # Calculate total videos and duration (filtered by channel)
    pipeline = [
        {'$match': {**base_filter}},
        {'$group': {
            '_id': None,
            'total_videos': {'$sum': '$video_count'},
            'total_duration': {'$sum': '$actual_duration_seconds'}
        }}
    ]
    
    aggregation_result = list(user_compilations_collection.aggregate(pipeline))
    if aggregation_result:
        stats['total_videos'] = aggregation_result[0].get('total_videos', 0)
        stats['total_duration'] = aggregation_result[0].get('total_duration', 0)
    
    # Pagination calculation
    total_pages = (total + per_page - 1) // per_page
    
    return frontend_manager.render_template('user_compilations',
                           compilations=compilations,
                           stats=stats,
                           page=page,
                           total_pages=total_pages,
                           total=total,
                           status_filter=status_filter)


@app.route('/api/compilation-preview', methods=['POST'])
@login_required
def api_compilation_preview():
    """Generate intelligent preview of compilation before creation"""
    try:
        data = request.get_json()
        duration = data.get('duration', 10)
        from_date = data.get('from_date')
        to_date = data.get('to_date')
        tags = data.get('tags', [])  # Get tags filter from request

        # Get current channel for filtering
        current_channel_id = get_current_channel_id()
        
        # Build query to filter videos by channel
        query = {}
        if current_channel_id:
            query['channel_id'] = current_channel_id
            print(f"🔍 DEBUG: Preview - Filtering videos by channel_id: {current_channel_id}")
        else:
            print(f"⚠️  WARNING: No channel_id in session for preview!")
        
        # Get available videos filtered by channel
        all_videos = list(videos_collection.find(query))

        # Get total available videos using the main categorization for other metrics
        categorized_videos = compilation_creator.categorize_videos_by_retention(
            all_videos, from_date, to_date, tags
        )

        # Calculate preview statistics
        total_available = sum(len(videos)
                              for videos in categorized_videos.values())

        if total_available == 0:
            return jsonify({
                'success': False,
                'error': 'No videos available with current filters'
            })

        # Calculate quality metrics from all available videos
        all_available_videos = []
        for category_videos in categorized_videos.values():
            all_available_videos.extend(category_videos)

        avg_retention = sum(v.get('retention_30s', 0) for v in all_available_videos) / \
            len(all_available_videos) if all_available_videos else 0
        avg_views = sum(v.get('view_count', 0) for v in all_available_videos) / \
            len(all_available_videos) if all_available_videos else 0

        # Estimate video count needed for duration
        avg_duration = sum(v.get('duration_seconds', 0) for v in all_available_videos) / \
            len(all_available_videos) if all_available_videos else 180
        estimated_video_count = max(2, int((duration * 60) / avg_duration))

        # Generate actual preview compilation to get the real video selection
        preview_compilation_result = compilation_creator.create_compilation(
            duration_minutes=duration,
            from_date=from_date,
            to_date=to_date,
            tags=tags,  # Pass tags filter to preview compilation
            user_id='preview',
            return_compilation_doc=True
        )

        # Analyze the quality distribution of videos actually selected for compilation
        compilation_counts = {'high': 0, 'good': 0, 'fair': 0, 'low': 0}
        compilation_retention_sum = 0
        compilation_views_sum = 0

        if preview_compilation_result and preview_compilation_result.get('success'):
            compilation_doc = preview_compilation_result.get('compilation_doc')
            if compilation_doc and 'timestamps' in compilation_doc:
                # Get details of videos actually selected for the compilation
                compilation_video_details = []
                for timestamp_entry in compilation_doc['timestamps']:
                    video_id = timestamp_entry.get('video_id')
                    if video_id:
                        video_detail = videos_collection.find_one({'video_id': video_id})
                        if video_detail:
                            compilation_video_details.append(video_detail)

                # Count quality distribution of selected videos
                for video in compilation_video_details:
                    retention_rate = video.get('retention_30s', 0)
                    compilation_retention_sum += retention_rate
                    compilation_views_sum += video.get('view_count', 0)

                    if retention_rate > 75:
                        compilation_counts['high'] += 1
                    elif retention_rate > 50:
                        compilation_counts['good'] += 1
                    elif retention_rate > 25:
                        compilation_counts['fair'] += 1
                    else:
                        compilation_counts['low'] += 1

                # Update metrics to use actual compilation video data
                if compilation_video_details:
                    avg_retention = compilation_retention_sum / len(compilation_video_details)
                    avg_views = compilation_views_sum / len(compilation_video_details)
                    estimated_video_count = len(compilation_video_details)

        # Quality score calculation (1-10 scale)
        quality_score = min(10, max(1,
                                    # Retention factor (0-10)
                                    (avg_retention / 10) +
                                    # Availability factor
                                    (3 if total_available > estimated_video_count * 2 else 1) +
                                    (2 if len(categorized_videos.get(VideoCategory.TOP_25_PERCENT, []))
                                        > 0 else 0)  # Quality factor
                                    ))

        return jsonify({
            'success': True,
            'total_available': total_available,
            'estimated_video_count': estimated_video_count,
            'avg_retention': round(avg_retention, 1),
            'avg_views': int(avg_views),
            'quality_score': round(quality_score, 1),
            'from_date': from_date,
            'category_counts': {
                'top_25': compilation_counts['high'],    # High: >75%
                'second_25': compilation_counts['good'],  # Good: >50%
                'third_25': compilation_counts['fair'],   # Fair: >25%
                'bottom_25': compilation_counts['low']    # Low: <25%
            }
        })

    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Preview generation failed: {str(e)}'
        })


@app.route('/compilation-preview/<compilation_id>')
@login_required
def compilation_preview(compilation_id):
    """Detailed compilation preview with export options and analytics"""
    try:
        # Get compilation details with enhanced information
        compilation_details = compilation_creator.get_compilation_preview(
            compilation_id)

        if not compilation_details:
            return "Compilation not found", 404

        # Calculate additional analytics for preview
        timestamps = compilation_details.get('timestamps', [])
        analytics = None

        if timestamps:
            # Analyze video quality distribution
            retention_rates = []
            view_counts = []

            for timestamp_entry in timestamps:
                video_details = videos_collection.find_one({
                    'video_id': timestamp_entry['video_id']
                })

                if video_details:
                    retention_rates.append(video_details.get(
                        'retention_30s', 0))
                    view_counts.append(video_details.get('view_count', 0))

            if retention_rates:  # Only calculate if we have data
                analytics = {
                    'avg_retention': sum(retention_rates) / len(retention_rates),
                    'total_original_views': sum(view_counts),
                    'min_retention': min(retention_rates),
                    'max_retention': max(retention_rates),
                    'quality_distribution': {
                        'high': len([r for r in retention_rates if r >= 70]),
                        'medium': len([r for r in retention_rates if 50 <= r < 70]),
                        'low': len([r for r in retention_rates if r < 50])
                    }
                }

                compilation_details['analytics'] = analytics

        # Convert ObjectId to string for template
        if '_id' in compilation_details:
            compilation_details['_id'] = str(compilation_details['_id'])

        # Use Flask's render_template instead of frontend_manager
        return render_template('compilation_preview.html',
                               compilation=compilation_details,
                               compilation_id=compilation_id,
                               analytics=analytics,
                               now=datetime.now())

    except Exception as e:
        print(f"Error in compilation_preview: {str(e)}")  # Debug logging
        return f"Error loading compilation preview: {str(e)}", 500


@app.route('/edit-compilation/<compilation_id>')
@login_required
def edit_compilation(compilation_id):
    """Edit compilation page with video management features"""
    try:
        # Get compilation details
        compilation_details = compilation_creator.get_compilation_preview(compilation_id)

        if not compilation_details:
            return "Compilation not found", 404

        # Calculate analytics for preview (similar to compilation_preview)
        timestamps = compilation_details.get('timestamps', [])
        analytics = None

        if timestamps:
            retention_rates = []
            view_counts = []

            for timestamp_entry in timestamps:
                video_details = videos_collection.find_one({
                    'video_id': timestamp_entry['video_id']
                })

                if video_details:
                    retention_rates.append(video_details.get('retention_30s', 0))
                    view_counts.append(video_details.get('view_count', 0))

            if retention_rates:
                analytics = {
                    'avg_retention': sum(retention_rates) / len(retention_rates),
                    'total_original_views': sum(view_counts),
                    'min_retention': min(retention_rates),
                    'max_retention': max(retention_rates),
                    'quality_distribution': {
                        'high': len([r for r in retention_rates if r >= 70]),
                        'medium': len([r for r in retention_rates if 50 <= r < 70]),
                        'low': len([r for r in retention_rates if r < 50])
                    }
                }

        # Convert ObjectId to string for template
        if '_id' in compilation_details:
            compilation_details['_id'] = str(compilation_details['_id'])

        return render_template('edit_compilation.html',
                               compilation=compilation_details,
                               compilation_id=compilation_id,
                               analytics=analytics,
                               now=datetime.now())

    except Exception as e:
        print(f"Error in edit_compilation: {str(e)}")
        return f"Error loading edit page: {str(e)}", 500


# ==================== COMPILATION EDIT API ENDPOINTS ====================

@app.route('/api/compilation/<compilation_id>/update', methods=['POST'])
@login_required
def api_update_compilation(compilation_id):
    """Enhanced compilation update with comprehensive video stats and metadata management"""
    try:
        data = request.get_json()
        new_timestamps = data.get('timestamps', [])
        new_title = data.get('title')
        duration_rounded = data.get('duration_rounded')

        if not new_timestamps:
            return jsonify({
                'success': False,
                'error': 'No timestamps provided'
            }), 400

        # Get current compilation to compare changes
        current_compilation = user_compilations_collection.find_one({
            '_id': ObjectId(compilation_id)
        })

        if not current_compilation:
            return jsonify({
                'success': False,
                'error': 'Compilation not found'
            }), 404

        current_timestamps = current_compilation.get('timestamps', [])
        current_first_video_id = current_timestamps[0].get('video_id') if current_timestamps else None
        new_first_video_id = new_timestamps[0].get('video_id') if new_timestamps else None

        # Calculate new compilation duration
        total_duration = 0
        for i, timestamp in enumerate(new_timestamps):
            video_duration = timestamp.get('original_duration', 0)
            total_duration += video_duration
            if i < len(new_timestamps) - 1:  # Add transition time except for last video
                total_duration += 2

        # If duration_rounded not provided, calculate it
        if duration_rounded is None:
            duration_rounded = max(5, round(total_duration / 60 / 5) * 5)

        # Handle title updates
        title_to_set = None
        first_video_changed = False
        
        if new_title and new_title.strip():
            # Manual title change by user
            title_to_set = new_title.strip()[:100]  # Enforce 100 character limit
        elif current_first_video_id != new_first_video_id and new_first_video_id:
            # First video changed - auto-update title based on new first video
            first_video = videos_collection.find_one({'video_id': new_first_video_id})
            if first_video:
                first_video_title = first_video.get('title', 'Untitled Video')
                # Remove existing suffixes to avoid duplication
                suffixes_to_remove = [
                    " | Mega Compilation | D Billions Kids Songs",
                    " | D Billions Kids Songs"
                ]
                clean_title = first_video_title
                for suffix in suffixes_to_remove:
                    if clean_title.endswith(suffix):
                        clean_title = clean_title[:-len(suffix)]
                title_to_set = f"{clean_title} | Mega Compilation | D Billions Kids Songs"
                first_video_changed = True

        # Prepare update fields
        update_fields = {
            'timestamps': new_timestamps,
            'updated_at': datetime.utcnow(),
            'video_count': len(new_timestamps),
            'actual_duration_seconds': total_duration,
            'duration_rounded': duration_rounded
        }
        
        if title_to_set:
            update_fields['title'] = title_to_set

        # Update the compilation
        result = user_compilations_collection.update_one(
            {'_id': ObjectId(compilation_id)},
            {'$set': update_fields}
        )

        if result.matched_count == 0:
            return jsonify({
                'success': False,
                'error': 'Compilation not found'
            }), 404

        # Update video usage statistics
        updated_video_ids = set()
        
        # Get all video IDs from new timestamps
        new_video_ids = {ts.get('video_id') for ts in new_timestamps if ts.get('video_id')}
        
        # Get all video IDs from old timestamps  
        old_video_ids = {ts.get('video_id') for ts in current_timestamps if ts.get('video_id')}
        
        # Videos that are still in compilation (may have changed position)
        still_in_compilation = new_video_ids.intersection(old_video_ids)
        
        # Videos that were added
        added_videos = new_video_ids - old_video_ids
        
        # Videos that were removed
        removed_videos = old_video_ids - new_video_ids
        
        print(f"🔄 Video stats update - Added: {len(added_videos)}, Removed: {len(removed_videos)}, Position changes: {len(still_in_compilation)}")

        # Update usage statistics for all affected videos
        usage_tracker = VideoUsageTracker(
            compilations_collection, user_compilations_collection, videos_collection
        )
        
        # Update stats for videos that are still in compilation (position might have changed)
        for video_id in still_in_compilation:
            try:
                usage_tracker.update_video_usage_stats(video_id)
                updated_video_ids.add(video_id)
            except Exception as e:
                print(f"⚠️  Error updating stats for video {video_id}: {e}")

        # Update stats for added videos (increase usage)
        for video_id in added_videos:
            try:
                usage_tracker.update_video_usage_stats(video_id)
                updated_video_ids.add(video_id)
            except Exception as e:
                print(f"⚠️  Error updating stats for added video {video_id}: {e}")

        # Update stats for removed videos (decrease usage)
        for video_id in removed_videos:
            try:
                usage_tracker.update_video_usage_stats(video_id)
                updated_video_ids.add(video_id)
            except Exception as e:
                print(f"⚠️  Error updating stats for removed video {video_id}: {e}")

        # Prepare response
        response_data = {
            'success': True,
            'message': 'Compilation updated successfully',
            'updated_fields': {
                'video_count': len(new_timestamps),
                'duration_rounded': duration_rounded,
                'actual_duration_seconds': total_duration,
                'title_updated': title_to_set is not None,
                'first_video_changed': first_video_changed
            },
            'video_stats_updated': len(updated_video_ids)
        }
        
        if title_to_set:
            response_data['new_title'] = title_to_set

        return jsonify(response_data)

    except Exception as e:
        print(f"❌ Error in api_update_compilation: {str(e)}")
        return jsonify({
            'success': False,
            'error': f'Failed to update compilation: {str(e)}'
        }), 500


@app.route('/api/available-videos/<compilation_id>')
@login_required
def api_available_videos(compilation_id):
    """Get list of videos that can be added to compilation with improved search support"""
    try:
        # Get current compilation to exclude already included videos
        current_compilation = user_compilations_collection.find_one({
            '_id': ObjectId(compilation_id)
        })

        if not current_compilation:
            return jsonify({
                'success': False,
                'error': 'Compilation not found'
            }), 404

        # Get list of video_ids already in compilation
        existing_video_ids = set()
        for timestamp in current_compilation.get('timestamps', []):
            existing_video_ids.add(timestamp.get('video_id'))

        # Get search parameters
        search_query = request.args.get('search', '').strip()
        page = request.args.get('page', 1, type=int)
        per_page = min(request.args.get('per_page', 100, type=int), 200)  # Limit to 200 per page max
        
        print(f"🔍 Search query: '{search_query}', page: {page}, per_page: {per_page}")
        
        # Build base query - exclude compilations and already included videos
        query = {
            'is_compilation': False,
            'video_id': {'$nin': list(existing_video_ids)}
        }
        
        # Channel filtering - only show videos from current channel
        current_channel_id = get_current_channel_id()
        if current_channel_id:
            query['channel_id'] = current_channel_id

        # Add enhanced search functionality
        if search_query:
            # Create more flexible search patterns
            import re
            
            # Split search query into individual words for better matching
            search_words = [word.strip() for word in search_query.split() if word.strip()]
            
            if len(search_words) == 1:
                # Single word search - use flexible matching
                word = search_words[0]
                escaped_word = re.escape(word)
                
                query['$or'] = [
                    # Title contains the word (flexible matching)
                    {'title': {'$regex': escaped_word, '$options': 'i'}},
                    # Video ID contains or matches the word
                    {'video_id': {'$regex': escaped_word, '$options': 'i'}},
                ]
            else:
                # Multi-word search - require all words to appear in title or video_id
                word_patterns = []
                for word in search_words:
                    escaped_word = re.escape(word)
                    word_patterns.append({
                        '$or': [
                            {'title': {'$regex': escaped_word, '$options': 'i'}},
                            {'video_id': {'$regex': escaped_word, '$options': 'i'}},
                        ]
                    })
                
                query['$and'] = word_patterns
            
            print(f"🔍 Built query: {query}")

        # Get total count for pagination
        total_count = videos_collection.count_documents(query)
        print(f"📊 Total matching videos: {total_count}")

        # Calculate skip value
        skip = (page - 1) * per_page

        # Get videos with pagination and sorting by relevance for search results
        if search_query:
            # If searching, sort by relevance (title matches first)
            videos = list(videos_collection.find(query)
                         .sort([
                             ('title', 1),  # Title ascending for consistent ordering
                             ('published_at', -1)  # Most recent first
                         ])
                         .skip(skip)
                         .limit(per_page))
        else:
            # Default sorting - most recent first
            videos = list(videos_collection.find(query)
                         .sort('published_at', -1)
                         .skip(skip)
                         .limit(per_page))

        # Convert to JSON-serializable format
        available_videos = []
        for video in videos:
            available_videos.append({
                'video_id': video.get('video_id'),
                'title': video.get('title', ''),
                'description': video.get('description', ''),
                'thumbnail_url': video.get('thumbnail_url', ''),
                'duration_seconds': video.get('duration_seconds', 0),
                'view_count': video.get('view_count', 0),
                'like_count': video.get('like_count', 0),
                'retention_30s': video.get('retention_30s', 0),
                'published_at': video.get('published_at', '')
            })

        print(f"📄 Returning {len(available_videos)} videos for page {page}")

        return jsonify({
            'success': True,
            'videos': available_videos,
            'total_count': total_count,
            'page': page,
            'per_page': per_page,
            'search_query': search_query,
            'has_more': (page * per_page) < total_count
        })

    except Exception as e:
        print(f"❌ Error in api_available_videos: {str(e)}")
        return jsonify({
            'success': False,
            'error': f'Failed to get available videos: {str(e)}'
        }), 500


# ==================== COMPILATION EXPORT ROUTES ====================

@app.route('/api/compilation/<compilation_id>/export', methods=['POST'])
@login_required
def api_export_compilation(compilation_id):
    """Advanced compilation export with multiple format options"""
    try:
        export_format = request.json.get('format', 'txt') if request.is_json else 'txt'
        include_analytics = request.json.get('include_analytics', True) if request.is_json else True

        if export_format == 'json':
            result = compilation_exporter.export_compilation_to_json(compilation_id)
        else:
            result = compilation_exporter.export_compilation_to_txt(
                compilation_id,
                include_analytics=include_analytics
            )

        # Add download URL for frontend compatibility
        if result.get('success') and result.get('filename'):
            result['download_url'] = f"/download-export/{result['filename']}"

        return jsonify(result)

    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Export failed: {str(e)}'
        })


@app.route('/download-export/<filename>')
@login_required
def download_export(filename):
    """Secure file download endpoint with validation"""
    try:
        # Validate filename to prevent directory traversal
        if '..' in filename or '/' in filename or '\\' in filename:
            return "Invalid filename", 400
        
        export_path = os.path.join(compilation_exporter.export_directory, filename)
        
        if not os.path.exists(export_path):
            return "File not found", 404
        
        return send_file(
            export_path,
            as_attachment=True,
            download_name=filename,
            mimetype='text/plain' if filename.endswith('.txt') else 'application/json'
        )
        
    except Exception as e:
        return f"Download failed: {str(e)}", 500


@app.route('/api/export-history')
@login_required
def api_export_history():
    """Get comprehensive export history with analytics"""
    try:
        compilation_id = request.args.get('compilation_id')
        history = compilation_exporter.get_export_history(compilation_id)
        
        return jsonify({
            'success': True,
            'history': history,
            'total_exports': len(history)
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })


# ==================== COMPILATION STATUS MANAGEMENT ====================

@app.route('/api/compilation/<compilation_id>/publish', methods=['POST'])
@login_required
def api_publish_compilation(compilation_id):
    """Publish a compilation with comprehensive validation"""
    try:
        success = compilation_creator.update_compilation_status(
            compilation_id, 
            CompilationStatus.PUBLISHED
        )
        
        if success:
            return jsonify({
                'success': True,
                'message': 'Compilation published successfully'
            })
        else:
            return jsonify({
                'success': False,
                'error': 'Failed to publish compilation'
            })
            
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Publishing failed: {str(e)}'
        })


@app.route('/api/compilation/<compilation_id>/delete', methods=['DELETE'])
@login_required
def api_delete_compilation(compilation_id):
    """Delete a compilation with safety checks"""
    try:
        success = compilation_creator.delete_compilation(compilation_id)
        
        if success:
            return jsonify({
                'success': True,
                'message': 'Compilation deleted successfully'
            })
        else:
            return jsonify({
                'success': False,
                'error': 'Cannot delete published compilation or compilation not found'
            })
            
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Deletion failed: {str(e)}'
        })


@app.route('/api/compilation-status', methods=['POST'])
@login_required
def api_change_compilation_status():
    """Change compilation status with workflow validation"""
    try:
        data = request.get_json()
        compilation_id = data.get('compilation_id')
        new_status = data.get('status')
        done_by = data.get('done_by')

        if not compilation_id or not new_status:
            return jsonify({
                'success': False,
                'error': 'Missing compilation_id or status'
            })

        # Map string status to enum
        status_mapping = {
            'generated': CompilationStatus.GENERATED,
            'to_do': CompilationStatus.TO_DO,
            'ready': CompilationStatus.READY,
            'uploaded': CompilationStatus.UPLOADED
        }

        if new_status not in status_mapping:
            return jsonify({
                'success': False,
                'error': f'Invalid status: {new_status}'
            })

        # Update the status
        success = compilation_creator.update_compilation_status(
            compilation_id, status_mapping[new_status], done_by
        )

        if success:
            message = f'Compilation status changed to {new_status}'
            if done_by and new_status == 'ready':
                message += f' by {done_by}'
            return jsonify({
                'success': True,
                'message': message
            })
        else:
            return jsonify({
                'success': False,
                'error': 'Status transition not allowed or compilation not found'
            })

    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Status change failed: {str(e)}'
        })


# ==================== REFRESH STATISTICS ROUTES ====================

@app.route('/api/refresh-all-stats', methods=['POST'])
@login_required
def api_refresh_all_stats():
    """Recalculate all compilation statistics and usage data"""
    try:
        print("🔄 Starting comprehensive statistics refresh...")

        # Process all compilations
        print("   📊 Processing all compilations...")
        processing_results = compilation_manager.process_all_compilations()

        # Recalculate all statistics
        print("   📈 Recalculating all usage statistics...")
        tracker = VideoUsageTracker(
            compilations_collection, user_compilations_collection, videos_collection)
        stats_result = tracker.recalculate_all_stats()

        print("   ✅ Statistics refresh completed")

        return jsonify({
            'success': True,
            'message': 'All statistics have been refreshed successfully',
            'processing_results': processing_results,
            'stats_result': stats_result
        })

    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Statistics refresh failed: {str(e)}'
        })


# ==================== ENHANCED ANALYTICS ROUTES ====================

@app.route('/compilation-analytics')
@login_required
def compilation_analytics_dashboard():
    """Advanced analytics dashboard for compilation performance and insights"""
    # Get comprehensive compilation statistics
    user_comp_stats = compilation_creator.get_compilation_statistics()
    auto_comp_stats = compilation_manager.get_compilation_statistics()
    
    # Combine statistics for comprehensive view
    combined_stats = {
        'user_compilations': user_comp_stats,
        'auto_compilations': auto_comp_stats,
        'total_compilations': (
            user_compilations_collection.count_documents({}) +
            compilations_collection.count_documents({})
        )
    }
    
    # Get trending video usage data
    trending_videos = list(videos_collection.find({
        'user_compilation_usage.total_inclusions': {'$gt': 0}
    }).sort('user_compilation_usage.total_inclusions', -1).limit(20))
    
    return frontend_manager.render_template('compilation_analytics',
                           stats=combined_stats,
                           trending_videos=trending_videos)


# ==================== MAINTENANCE AND UTILITY ROUTES ====================

@app.route('/api/cleanup-exports', methods=['POST'])
@login_required
def api_cleanup_exports():
    """Clean up old export files to manage storage"""
    try:
        days_old = request.json.get('days_old', 30) if request.is_json else 30
        result = compilation_exporter.cleanup_old_exports(days_old)
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Cleanup failed: {str(e)}'
        })


# ==================== SETTINGS PAGE ROUTES ====================

@app.route('/settings')
@login_required
def settings():
    """Settings page with blacklist and general configuration"""
    try:
        # Get all blacklisted videos
        blacklisted_videos = list(blacklist_collection.find({}).sort('added_date', -1))
        
        # Get video details for blacklisted videos
        for item in blacklisted_videos:
            video = videos_collection.find_one({'video_id': item['video_id']})
            item['title'] = video.get('title', 'Unknown Title') if video else 'Unknown Title'
            item['_id'] = str(item['_id'])
        
        # Get total video count
        total_videos = videos_collection.count_documents({})
        
        return render_template('settings.html',
                               blacklisted_videos=blacklisted_videos,
                               total_videos=total_videos)
    except Exception as e:
        return f"Error loading settings: {str(e)}", 500


@app.route('/api/settings/blacklist', methods=['POST'])
@login_required
def api_add_to_blacklist():
    """Add video ID to blacklist"""
    try:
        data = request.get_json()
        video_id = data.get('video_id', '').strip()
        
        if not video_id:
            return jsonify({'success': False, 'error': 'Video ID is required'}), 400
        
        # Check if video exists
        video = videos_collection.find_one({'video_id': video_id})
        if not video:
            return jsonify({'success': False, 'error': 'Video not found'}), 404
        
        # Check if already blacklisted
        existing = blacklist_collection.find_one({'video_id': video_id})
        if existing:
            return jsonify({'success': False, 'error': 'Video is already blacklisted'}), 400
        
        # Add to blacklist
        blacklist_item = {
            'video_id': video_id,
            'added_date': datetime.utcnow(),
            'added_by': session.get('username', 'admin')
        }
        
        result = blacklist_collection.insert_one(blacklist_item)
        
        if result.inserted_id:
            return jsonify({'success': True, 'message': 'Video added to blacklist'})
        else:
            return jsonify({'success': False, 'error': 'Failed to add to blacklist'}), 500
            
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/settings/blacklist/<video_id>', methods=['DELETE'])
@login_required
def api_remove_from_blacklist(video_id):
    """Remove video ID from blacklist"""
    try:
        # URL decode the video_id
        import urllib.parse
        video_id = urllib.parse.unquote(video_id)
        
        result = blacklist_collection.delete_one({'video_id': video_id})
        
        if result.deleted_count > 0:
            return jsonify({'success': True, 'message': 'Video removed from blacklist'})
        else:
            return jsonify({'success': False, 'error': 'Video not found in blacklist'}), 404
            
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/settings/blacklist/clear', methods=['POST'])
@login_required
def api_clear_blacklist():
    """Clear entire blacklist"""
    try:
        result = blacklist_collection.delete_many({})
        
        return jsonify({
            'success': True, 
            'message': f'Cleared {result.deleted_count} videos from blacklist'
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/settings/blacklist')
@login_required
def api_get_blacklist():
    """Get all blacklisted videos"""
    try:
        blacklisted_videos = list(blacklist_collection.find({}).sort('added_date', -1))
        
        # Get video details
        for item in blacklisted_videos:
            video = videos_collection.find_one({'video_id': item['video_id']})
            item['title'] = video.get('title', 'Unknown Title') if video else 'Unknown Title'
            item['_id'] = str(item['_id'])
        
        return jsonify({
            'success': True,
            'blacklisted_videos': blacklisted_videos
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ==================== TAG MANAGEMENT API ENDPOINTS ====================

@app.route('/api/tags/search')
@login_required
def api_search_tags():
    """API endpoint to search for existing tags"""
    query = request.args.get('query', '').strip()
    if not query:
        return jsonify({'tags': []})

    # Search for tags that contain the query
    pipeline = [
        {'$unwind': '$tags'},
        {'$match': {'tags': {'$regex': query, '$options': 'i'}}},
        {'$group': {'_id': '$tags', 'count': {'$sum': 1}}},
        {'$sort': {'count': -1, '_id': 1}},
        {'$limit': 10}
    ]

    results = list(videos_collection.aggregate(pipeline))
    tags = [result['_id'] for result in results]

    return jsonify({'tags': tags})


@app.route('/api/tags/all')
@login_required
def api_get_all_tags():
    """API endpoint to get all available tags without query requirement"""
    # Get all unique tags with their counts
    pipeline = [
        {'$unwind': '$tags'},
        {'$group': {'_id': '$tags', 'count': {'$sum': 1}}},
        {'$sort': {'count': -1, '_id': 1}}
    ]

    results = list(videos_collection.aggregate(pipeline))
    tags = [result['_id'] for result in results if result['_id']]  # Filter out empty tags

    return jsonify({'tags': tags})


@app.route('/api/video/<video_id>/tags', methods=['POST'])
@login_required
def api_add_tag(video_id):
    """API endpoint to add a tag to a video"""
    data = request.get_json()
    tag = data.get('tag', '').strip()
    
    if not tag:
        return jsonify({'error': 'Tag cannot be empty'}), 400

    # Find the video first
    video = videos_collection.find_one({'video_id': video_id})
    if not video:
        return jsonify({'error': 'Video not found'}), 404

    # Get current tags or initialize empty list
    current_tags = video.get('tags', [])
    
    # Check if tag already exists
    if tag in current_tags:
        return jsonify({'error': 'Tag already exists'}), 400

    # Add the new tag
    current_tags.append(tag)
    
    result = videos_collection.update_one(
        {'video_id': video_id},
        {
            '$set': {
                'tags': current_tags,
                'updated_at': datetime.utcnow()
            }
        }
    )

    if result.matched_count:
        return jsonify({'success': True})
    else:
        return jsonify({'error': 'Failed to add tag'}), 500


@app.route('/api/video/<video_id>/tags/<tag>', methods=['DELETE'])
@login_required
def api_remove_tag(video_id, tag):
    """API endpoint to remove a tag from a video"""
    # URL decode the tag
    import urllib.parse
    tag = urllib.parse.unquote(tag)

    # Find the video first
    video = videos_collection.find_one({'video_id': video_id})
    if not video:
        return jsonify({'error': 'Video not found'}), 404

    # Get current tags
    current_tags = video.get('tags', [])
    
    # Check if tag exists
    if tag not in current_tags:
        return jsonify({'error': 'Tag not found'}), 404

    # Remove the tag
    current_tags.remove(tag)
    
    result = videos_collection.update_one(
        {'video_id': video_id},
        {
            '$set': {
                'tags': current_tags,
                'updated_at': datetime.utcnow()
            }
        }
    )

    if result.matched_count:
        return jsonify({'success': True})
    else:
        return jsonify({'error': 'Failed to remove tag'}), 500


@app.route('/api/video/<video_id>/tags')
@login_required
def api_get_tags(video_id):
    """API endpoint to get all tags for a video"""
    video = videos_collection.find_one({'video_id': video_id})
    if not video:
        return jsonify({'error': 'Video not found'}), 404

    tags = video.get('tags', [])
    return jsonify({'tags': tags})


@app.route('/api/video/<video_id>/actor', methods=['POST'])
@login_required
def api_update_actor_status(video_id):
    """API endpoint to update actor status for a video"""
    data = request.get_json()
    actor_status = data.get('actor', False)
    
    # Find the video first
    video = videos_collection.find_one({'video_id': video_id})
    if not video:
        return jsonify({'error': 'Video not found'}), 404

    # Update the actor status
    result = videos_collection.update_one(
        {'video_id': video_id},
        {
            '$set': {
                'actor': bool(actor_status),
                'updated_at': datetime.utcnow()
            }
        }
    )

    if result.matched_count:
        return jsonify({'success': True, 'actor': bool(actor_status)})
    else:
        return jsonify({'error': 'Failed to update actor status'}), 500


if __name__ == '__main__':
    # Ensure export directory exists on startup
    os.makedirs('exports', exist_ok=True)
    
    # Start the enhanced application with comprehensive logging
    print("🚀 Starting DB Compilations project...")
    print("🌐 Access the application at: http://localhost:5002")
    
    app.run(debug=True, host='0.0.0.0', port=5002)
