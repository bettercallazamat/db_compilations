# Video Compilation Analytics

A Flask-based web application for managing video databases and analyzing compilation video usage patterns. This system automatically identifies compilation videos, extracts timestamps, and tracks video usage statistics across different compilation types.

## Features

### Core Functionality
- **Video Database Management**: Import and manage video metadata from JSON files
- **Automatic Compilation Detection**: Identifies compilation videos using keywords like "Mega Compilation" and "+ MORE"
- **Timestamp Extraction**: Parses video timestamps from descriptions to identify included videos
- **Usage Statistics**: Tracks how often individual videos appear in compilations
- **Duration-based Analytics**: Analyzes usage patterns by compilation duration (rounded to nearest 5 minutes)

### Advanced Analytics
- **First Video Tracking**: Monitors how often videos are used as the opening video in compilations
- **Duration-based Usage Reports**: Shows usage statistics segmented by compilation length (5min, 10min, 15min, etc.)
- **Year-over-year Analysis**: Tracks usage patterns over the last 12 months
- **Duplicate Detection**: Identifies potential duplicate compilations

## Project Structure

```
video-compilation-analytics/
├── parse_json_updated.py     # Main Flask application
├── compilation_parser.py     # Compilation detection and parsing logic
├── compilation_manager.py    # Compilation management and database operations
├── db_schema.py             # Database schema initialization and management
├── utils.py                 # Utility functions for analysis and maintenance
├── setup.py                 # Setup and initialization script
├── requirements.txt         # Python dependencies
├── templates/               # HTML templates directory
└── README.md               # This file
```

## Database Schema

### Videos Collection
```javascript
{
  "_id": ObjectId,
  "title": String,
  "video_id": String (unique),
  "published_at": String,
  "description": String,
  "thumbnail_url": String,
  "duration": String,
  "duration_seconds": Number,
  "view_count": Number,
  "like_count": Number,
  "comment_count": Number,
  "estimated_minutes_watched": Number,
  "average_view_duration": Number,
  "average_view_percentage": Number,
  "retention_30s": Number,
  "actor": Boolean,
  "tags": Array,
  "is_compilation": Boolean,
  "compilation_usage_stats": {
    "total_inclusions": Number,
    "first_video_count": Number,
    "usage_by_duration": {"10min": 5, "15min": 2},
    "first_video_by_duration": {"10min": 2, "15min": 1}
  },
  "created_at": Date,
  "updated_at": Date
}
```

### Compilations Collection
```javascript
{
  "_id": ObjectId,
  "original_video_id": ObjectId,  // Reference to videos collection
  "title": String,
  "video_id": String,
  "duration": Number,              // Duration in seconds
  "duration_rounded": Number,      // Rounded to nearest 5 minutes
  "timestamps": [
    {
      "timestamp": "0:00",
      "title": "Video Title"
    }
  ],
  "published_at": String,
  "view_count": Number,
  "like_count": Number,
  "created_at": Date,
  "updated_at": Date
}
```

## Installation and Setup

### Prerequisites
- Python 3.7 or higher
- MongoDB (running on localhost:27017)
- Flask and required Python packages

### Quick Setup
1. Clone or download the project files
2. Run the setup script:
   ```bash
   python setup.py
   ```

### Manual Setup
1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Ensure MongoDB is running:
   ```bash
   # On macOS with Homebrew
   brew services start mongodb-community
   
   # On Ubuntu/Debian
   sudo systemctl start mongod
   
   # On Windows
   net start MongoDB
   ```

3. Initialize the database:
   ```bash
   python utils.py init
   ```

4. Start the Flask application:
   ```bash
   python parse_json.py
   ```

5. Open your browser and go to: http://localhost:5002

## Usage

### Importing Video Data
1. Navigate to `/import` in your web browser
2. Upload a JSON file containing video data
3. The system will automatically process compilations after import

### Processing Compilations
To manually process all videos for compilations:
```bash
# Via command line
python utils.py analyze
```

### API Endpoints

#### Videos
- `GET /api/videos` - Get paginated video list
- `PUT /api/video/<video_id>` - Update video metadata

#### Compilations
- `GET /api/compilations` - Get paginated compilation list
- `GET /api/compilation/<compilation_id>` - Get compilation details
- `POST /process_compilations` - Process all videos for compilations
- `POST /compilation/<compilation_id>/delete` - Delete compilation

## Command Line Tools

The `utils.py` script provides several command-line utilities:

```bash
# Initialize database schema
python utils.py init

# Analyze compilation keywords in descriptions
python utils.py analyze

# Generate comprehensive usage report
python utils.py report -o usage_report.json

# Find duplicate compilations
python utils.py duplicates

# Export compilations data
python utils.py export -o compilations_backup.json

# Validate compilation data integrity
python utils.py validate
```
