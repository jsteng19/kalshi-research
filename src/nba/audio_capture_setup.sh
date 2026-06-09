#!/bin/bash
# NBA Audio Capture Setup Script
# Sets up BlackHole and multi-output device for parallel game recording

set -e

echo "🎙️  NBA Audio Capture Setup"
echo "============================"

# Check if BlackHole is installed
if brew list --formula | grep -q "^blackhole-2ch$"; then
    echo "✅ BlackHole 2ch is already installed"
else
    echo "📦 Installing BlackHole 2ch..."
    brew install blackhole-2ch
    echo "✅ BlackHole installed"
    echo ""
    echo "⚠️  IMPORTANT: You may need to restart your Mac for BlackHole to work properly"
    echo "   After restart, run this script again to complete setup"
    exit 0
fi

# Check if we can access BlackHole
if ffmpeg -f avfoundation -list_devices true -i "" 2>&1 | grep -q "BlackHole"; then
    echo "✅ BlackHole is accessible"
else
    echo "⚠️  BlackHole not detected by FFmpeg"
    echo "   You may need to:"
    echo "   1. Restart your Mac"
    echo "   2. Grant microphone permissions to Terminal/iTerm"
    echo "   3. Check Audio MIDI Setup app"
fi

echo ""
echo "📋 Manual Setup Steps:"
echo "====================="
echo ""
echo "1. Open 'Audio MIDI Setup' app (Cmd+Space, type 'Audio MIDI Setup')"
echo ""
echo "2. Click the '+' button → 'Create Multi-Output Device'"
echo ""
echo "3. In the Multi-Output Device settings:"
echo "   - Check 'BlackHole 2ch'"
echo "   - Check your speakers/headphones (so you can still hear audio)"
echo "   - Name it 'NBA Recording'"
echo ""
echo "4. Go to System Settings → Sound → Output"
echo "   - Select 'NBA Recording' as your output device"
echo ""
echo "5. Test it:"
echo "   - Play some audio (YouTube, Spotify, etc.)"
echo "   - You should hear it AND it should be captured by BlackHole"
echo ""
echo "✅ Setup complete! You can now use the batch recording script."
echo ""
echo "💡 For parallel recording:"
echo "   - Use multiple browser windows (one per game)"
echo "   - Each window can play at 2x speed"
echo "   - The batch_recorder.py script handles multiple recordings"





