from flask import Flask, render_template, request, jsonify, redirect, url_for
from flask_pymongo import PyMongo
from bson.objectid import ObjectId
import json
from datetime import datetime
import os

app = Flask(__name__)

# MongoDB Configuration
app.config["MONGO_URI"] = "mongodb://localhost:27017/video_database"
mongo = PyMongo(app)

# Collection reference
videos_collection = mongo.db.videos


class VideoManager:
    @staticmethod
    def import_from_json(json_file_path):
        """Import videos from JSON file"""
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

                # Prepare video document
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
                    # Manual entry fields with default values
                    'actor': False,
                    'tags': [],
                    'is_compilation': False,
                    'created_at': datetime.utcnow(),
                    'updated_at': datetime.utcnow()
                }

                videos_collection.insert_one(video_doc)
                imported_count += 1

            return imported_count, len(videos)

        except Exception as e:
            return 0, 0, str(e)


@app.route('/')
def index():
    """Main page showing all videos"""
    page = request.args.get('page', 1, type=int)
    per_page = 10

    # Get filter parameters
    search_query = request.args.get('search', '')
    actor_filter = request.args.get('actor')
    compilation_filter = request.args.get('compilation')
    retention_filter = request.args.get('retention')
    tag_filter = request.args.get('tag')
    
    # Get sort parameters
    sort_column = request.args.get('sort', '')
    sort_order = request.args.get('order', 'desc')

    # Build MongoDB query
    query = {}
    if search_query:
        query['$or'] = [
            {'title': {'$regex': search_query, '$options': 'i'}},
            {'description': {'$regex': search_query, '$options': 'i'}}
        ]

    if actor_filter == 'true':
        query['actor'] = True
    elif actor_filter == 'false':
        query['actor'] = False

    if compilation_filter == 'true':
        query['is_compilation'] = True
    elif compilation_filter == 'false':
        query['is_compilation'] = False

    if retention_filter:
        if retention_filter == 'high':
            query['retention_30s'] = {'$gte': 70}
        elif retention_filter == 'medium':
            query['retention_30s'] = {'$gte': 50, '$lt': 70}
        elif retention_filter == 'low':
            query['retention_30s'] = {'$lt': 50}

    if tag_filter:
        query['tags'] = tag_filter

    # Get total count for pagination
    total = videos_collection.count_documents(query)

    # Get videos with pagination
    videos = list(videos_collection.find(query)
                  .sort('published_at', -1)
                  .skip((page - 1) * per_page)
                  .limit(per_page))

    # Calculate pagination info
    total_pages = (total + per_page - 1) // per_page
    has_prev = page > 1
    has_next = page < total_pages

    # Get available tags for filter dropdown
    available_tags = get_available_tags()

    # Get quick stats for dashboard
    quick_stats = get_quick_stats()

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
                           available_tags=available_tags,
                           quick_stats=quick_stats)


def get_available_tags():
    """Get all available tags for filter dropdown"""
    pipeline = [
        {'$unwind': '$tags'},
        {'$group': {'_id': '$tags', 'count': {'$sum': 1}}},
        {'$sort': {'count': -1, '_id': 1}}
    ]
    
    results = list(videos_collection.aggregate(pipeline))
    return [result['_id'] for result in results]


def get_quick_stats():
    """Get quick statistics for dashboard"""
    # Total videos
    total_videos = videos_collection.count_documents({})
    
    # Total compilations
    compilations = videos_collection.count_documents({'is_compilation': True})
    
    # High retention videos (70%+)
    high_retention = videos_collection.count_documents({'retention_30s': {'$gte': 70}})
    
    # User compilations (would need user_compilations collection)
    user_compilations = 0
    try:
        user_compilations = mongo.db.user_compilations.count_documents({})
    except:
        pass  # Collection might not exist yet
    
    return {
        'total_videos': total_videos,
        'compilations': compilations,
        'high_retention': high_retention,
        'user_compilations': user_compilations
    }


@app.route('/video/<video_id>')
def video_detail(video_id):
    """Show detailed view of a single video"""
    # Try to find by ObjectId first, then by video_id
    try:
        video = videos_collection.find_one({'_id': ObjectId(video_id)})
    except:
        video = videos_collection.find_one({'video_id': video_id})
    
    if not video:
        return "Video not found", 404
    return render_template('video_detail.html', video=video)


@app.route('/video/<video_id>/edit', methods=['GET', 'POST'])
def edit_video(video_id):
    """Edit video details"""
    video = videos_collection.find_one({'_id': ObjectId(video_id)})
    if not video:
        return "Video not found", 404

    if request.method == 'POST':
        # Get form data
        actor = request.form.get('actor') == 'on'
        is_compilation = request.form.get('is_compilation') == 'on'
        tags_input = request.form.get('tags', '')
        tags = [tag.strip() for tag in tags_input.split(',') if tag.strip()]

        # Update video
        videos_collection.update_one(
            {'_id': ObjectId(video_id)},
            {
                '$set': {
                    'actor': actor,
                    'is_compilation': is_compilation,
                    'tags': tags,
                    'updated_at': datetime.utcnow()
                }
            }
        )

        return redirect(url_for('video_detail', video_id=video_id))

    return render_template('edit_video.html', video=video)


@app.route('/import', methods=['GET', 'POST'])
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


@app.route('/api/videos')
def api_videos():
    """API endpoint to get videos as JSON"""
    page = request.args.get('page', 1, type=int)
    per_page = min(request.args.get('per_page', 10, type=int), 100)

    videos = list(videos_collection.find()
                  .sort('published_at', -1)
                  .skip((page - 1) * per_page)
                  .limit(per_page))

    # Convert ObjectId to string for JSON serialization
    for video in videos:
        video['_id'] = str(video['_id'])

    return jsonify({
        'videos': videos,
        'page': page,
        'per_page': per_page
    })


@app.route('/api/video/<video_id>', methods=['PUT'])
def api_update_video(video_id):
    """API endpoint to update video"""
    data = request.get_json()

    update_fields = {}
    if 'actor' in data:
        update_fields['actor'] = bool(data['actor'])
    if 'is_compilation' in data:
        update_fields['is_compilation'] = bool(data['is_compilation'])
    if 'tags' in data:
        update_fields['tags'] = data['tags'] if isinstance(
            data['tags'], list) else []

    if update_fields:
        update_fields['updated_at'] = datetime.utcnow()

        result = videos_collection.update_one(
            {'_id': ObjectId(video_id)},
            {'$set': update_fields}
        )

        if result.matched_count:
            return jsonify({'success': True})
        else:
            return jsonify({'error': 'Video not found'}), 404

    return jsonify({'error': 'No valid fields to update'}), 400


@app.route('/api/tags/search')
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
def api_get_tags(video_id):
    """API endpoint to get all tags for a video"""
    video = videos_collection.find_one({'video_id': video_id})
    if not video:
        return jsonify({'error': 'Video not found'}), 404

    tags = video.get('tags', [])
    return jsonify({'tags': tags})


@app.route('/stats')
def stats():
    """Show statistics about the video collection"""
    pipeline = [
        {
            '$group': {
                '_id': None,
                'total_videos': {'$sum': 1},
                'total_views': {'$sum': '$view_count'},
                'total_likes': {'$sum': '$like_count'},
                'total_minutes_watched': {'$sum': '$estimated_minutes_watched'},
                'actor_videos': {'$sum': {'$cond': ['$actor', 1, 0]}},
                'compilation_videos': {'$sum': {'$cond': ['$is_compilation', 1, 0]}},
                'avg_duration': {'$avg': '$duration_seconds'},
                'avg_views': {'$avg': '$view_count'},
                'avg_retention': {'$avg': '$average_view_percentage'}
            }
        }
    ]

    stats_result = list(videos_collection.aggregate(pipeline))
    stats_data = stats_result[0] if stats_result else {}

    # Get top tags
    tag_pipeline = [
        {'$unwind': '$tags'},
        {'$group': {'_id': '$tags', 'count': {'$sum': 1}}},
        {'$sort': {'count': -1}},
        {'$limit': 10}
    ]

    top_tags = list(videos_collection.aggregate(tag_pipeline))

    return render_template('stats.html', stats=stats_data, top_tags=top_tags)


if __name__ == '__main__':
    app.run(debug=True, port=5001)
