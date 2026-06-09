# NBA Audio Capture Setup Guide

## Quick Start

### 1. Initial Setup (One-time)

```bash
# Run the setup script
bash src/nba/audio_capture_setup.sh
```

This will:
- Install BlackHole 2ch (if not already installed)
- Guide you through Audio MIDI Setup configuration

### 2. Configure Audio MIDI Setup

1. Open **Audio MIDI Setup** app (Cmd+Space → "Audio MIDI Setup")
2. Click **"+"** → **"Create Multi-Output Device"**
3. Check both:
   - ✅ **BlackHole 2ch**
   - ✅ Your speakers/headphones (so you can still hear audio)
4. Name it **"NBA Recording"**
5. Go to **System Settings → Sound → Output**
6. Select **"NBA Recording"** as your output device

### 3. Test Setup

Play any audio (YouTube, Spotify, etc.). You should:
- ✅ Hear the audio (through your speakers)
- ✅ Be able to capture it with FFmpeg

Test capture:
```bash
ffmpeg -f avfoundation -i ":BlackHole 2ch" -t 10 -ar 16000 -ac 1 test.mp3
# Play some audio, then check test.mp3
```

## Recording Games

### Single Game

```bash
# 1. Open game in browser, start playback at 2x speed
# 2. In another terminal, start recording:
python src/nba/batch_recorder.py \
    --games "https://www.nba.com/game/atl-vs-phi-0022500304" \
    --output-dir data-nba/recordings/ \
    --duration 90
```

### Multiple Games in Parallel

**Option 1: Command line**
```bash
python src/nba/batch_recorder.py \
    --games \
        "https://www.nba.com/game/atl-vs-phi-0022500304" \
        "https://www.nba.com/game/lal-vs-gsw-0022500305" \
        "https://www.nba.com/game/bos-vs-mia-0022500306" \
    --output-dir data-nba/recordings/ \
    --duration 90
```

**Option 2: Game list file**
```bash
# Create games.txt:
# Hawks vs 76ers|https://www.nba.com/game/atl-vs-phi-0022500304
# Lakers vs Warriors|https://www.nba.com/game/lal-vs-gsw-0022500305

python src/nba/batch_recorder.py \
    --game-list games.txt \
    --output-dir data-nba/recordings/ \
    --duration 90
```

### Workflow for Parallel Recording

1. **Open games in separate browser windows**
   - Each game in its own window
   - Position them so you can see all are playing

2. **Start playback at 2x speed**
   - Each game should be playing at 2x
   - You should hear audio from all games

3. **Start the batch recorder**
   ```bash
   python src/nba/batch_recorder.py --games url1 url2 url3 --output-dir recordings/
   ```

4. **Press ENTER when prompted** (after all games are playing)

5. **Let it run** - The script will:
   - Record all games simultaneously
   - Show status updates every 10 seconds
   - Save each game to a separate file

6. **Stop early if needed**: Ctrl+C will gracefully stop all recordings

## Performance Tips

### Speed Optimization

- **2x playback** = ~1.25 hours per 2.5 hour game ✅ **Recommended**
- **3x playback** = ~50 minutes (test first - may have glitches)
- **4x+ playback** = Not recommended (audio quality degrades)
- **Note**: Higher speeds don't affect recorded audio quality - you're just capturing faster. The risk is browser/player glitches at high speeds.

### Parallel Recording Limits

**M4 MacBook Pro Performance:**
- **4-6 games in parallel** should be comfortable
- **6-8 games** possible (test your system)
- **M4 advantages**:
  - Excellent CPU performance for FFmpeg encoding
  - Unified memory helps with multiple processes
  - Fast SSD for concurrent writes

**Limiting factors**:
- **Browser rendering** (multiple videos at 2-3x speed) - main bottleneck
- **CPU** (FFmpeg encoding) - M4 handles this well
- **Disk I/O** (writing multiple files) - M4 SSD is fast
- **Memory** - Not usually an issue with M4's unified memory

**Testing your limits:**
Start with 3 games, then try 4, 5, 6. Watch for:
- Browser lag/stuttering
- FFmpeg errors in terminal
- System overheating

### Recommended Settings for ASR (Speech-to-Text)

**Optimal ASR settings:**
```bash
--sample-rate 16000  # Standard for Whisper, OpenAI, etc. (16kHz is sufficient)
--bitrate 64k        # More than enough for speech (saves space)
--format mp3         # Smaller files, same ASR accuracy
```

**Why these settings:**
- **16kHz sample rate**: Human speech is 0-8kHz, 16kHz captures everything needed
- **64k bitrate**: Speech doesn't need high bitrate (music does). 64k is fine.
- **MP3 format**: ASR models don't benefit from WAV - MP3 is smaller and just as accurate

**For Whisper specifically:**
- Whisper resamples to 16kHz internally anyway
- Higher sample rates just waste space
- Bitrate above 64k doesn't improve accuracy

**File size comparison** (2.5 hour game at 2x = 75 min recording):
- 16kHz/64k MP3: ~36 MB
- 16kHz/128k MP3: ~72 MB  
- 16kHz/64k WAV: ~144 MB (unnecessary)

## Output Files

Recordings are saved as:
```
output_dir/
├── g0022500304.mp3          # Game 1
├── g0022500305.mp3          # Game 2
├── g0022500306.mp3          # Game 3
└── recording_metadata.json  # Recording info
```

## Troubleshooting

### "BlackHole not detected"
- Restart your Mac (required after first install)
- Check Audio MIDI Setup → BlackHole 2ch is present
- Grant microphone permissions to Terminal

### "No audio captured"
- Verify Multi-Output Device is set as system output
- Check that audio is actually playing
- Test with: `ffmpeg -f avfoundation -i ":BlackHole 2ch" -t 5 test.mp3`

### "Recording stops early"
- Increase `--duration` (default is 90 minutes)
- Check disk space
- Check system logs for errors

### "Can't hear audio while recording"
- Make sure your speakers/headphones are checked in Multi-Output Device
- Verify system output is set to "NBA Recording"

## Advanced Usage

### Custom audio settings
```bash
python src/nba/batch_recorder.py \
    --games url1 url2 \
    --output-dir recordings/ \
    --sample-rate 22050 \
    --bitrate 192k \
    --format wav \
    --duration 120
```

### Monitor recording progress
The script shows status every 10 seconds. You can also check file sizes:
```bash
watch -n 5 'ls -lh recordings/*.mp3'
```

## Next Steps

After recording, use your existing transcription pipeline:
```bash
# Example with Whisper
python src/nfl/whisper_transcriber.py \
    --input-dir data-nba/recordings/ \
    --output-dir data-nba/transcripts/
```




