#!/bin/bash
# Validator Performance Profiling Script
# Usage: ./profile_validator.sh [duration] [output_prefix]

set -e

# Configuration
DURATION=${1:-60}  # Default 60 seconds
PREFIX=${2:-"validator_profile"}
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🔍 Gaia Validator Performance Profiler${NC}"
echo -e "${BLUE}=================================${NC}"

# Find validator process
echo -e "${YELLOW}Finding validator process...${NC}"
VALIDATOR_PID=$(pgrep -f "python.*validator" | head -1)

if [ -z "$VALIDATOR_PID" ]; then
    echo -e "${RED}❌ Validator process not found${NC}"
    echo "Make sure the validator is running and try again"
    exit 1
fi

echo -e "${GREEN}✅ Found validator PID: $VALIDATOR_PID${NC}"

# Check if py-spy is available
if ! command -v py-spy &> /dev/null; then
    echo -e "${RED}❌ py-spy not found${NC}"
    echo "Install with: pip install py-spy"
    exit 1
fi

echo -e "${GREEN}✅ py-spy available${NC}"

# Create output directory
mkdir -p profiling_output
cd profiling_output

echo -e "${BLUE}📊 Starting profiling for ${DURATION} seconds...${NC}"

# 1. Quick hotspot identification (real-time)
echo -e "${YELLOW}🔥 Capturing current hotspots...${NC}"
timeout 10s py-spy top --pid $VALIDATOR_PID --rate 100 --nonblocking > "${PREFIX}_hotspots_${TIMESTAMP}.txt" 2>&1 || true

# 2. Generate flame graph for detailed analysis
echo -e "${YELLOW}🔥 Generating flame graph...${NC}"
py-spy record -o "${PREFIX}_flamegraph_${TIMESTAMP}.svg" \
    --duration $DURATION \
    --pid $VALIDATOR_PID \
    --rate 100 \
    --subprocesses

# 3. Function-level analysis
echo -e "${YELLOW}🔥 Capturing function-level statistics...${NC}"
timeout 15s py-spy top --pid $VALIDATOR_PID --rate 200 --function > "${PREFIX}_functions_${TIMESTAMP}.txt" 2>&1 || true

# 4. System resource usage
echo -e "${YELLOW}🔥 Capturing system resources...${NC}"
ps -p $VALIDATOR_PID -o pid,ppid,cmd,%mem,%cpu,etime > "${PREFIX}_resources_${TIMESTAMP}.txt"

# Get memory info
if command -v pmap &> /dev/null; then
    pmap $VALIDATOR_PID > "${PREFIX}_memory_${TIMESTAMP}.txt" 2>&1 || true
fi

# Generate summary report
echo -e "${YELLOW}📝 Generating summary report...${NC}"
cat > "${PREFIX}_summary_${TIMESTAMP}.md" << EOF
# Validator Performance Profile Report

**Generated:** $(date)
**PID:** $VALIDATOR_PID
**Duration:** ${DURATION} seconds

## Files Generated

1. **${PREFIX}_hotspots_${TIMESTAMP}.txt** - Real-time CPU hotspots
2. **${PREFIX}_flamegraph_${TIMESTAMP}.svg** - Interactive flame graph (open in browser)
3. **${PREFIX}_functions_${TIMESTAMP}.txt** - Function-level statistics
4. **${PREFIX}_resources_${TIMESTAMP}.txt** - System resource usage
5. **${PREFIX}_memory_${TIMESTAMP}.txt** - Memory mapping details

## How to Analyze

### 1. Check Hotspots
\`\`\`bash
cat ${PREFIX}_hotspots_${TIMESTAMP}.txt
\`\`\`

Look for functions with high **%Own** percentages.

### 2. Open Flame Graph
\`\`\`bash
firefox ${PREFIX}_flamegraph_${TIMESTAMP}.svg
# or
google-chrome ${PREFIX}_flamegraph_${TIMESTAMP}.svg
\`\`\`

- **Wide bars** = CPU-intensive functions
- **Tall stacks** = Deep call chains
- **Red/Orange** = Hot functions

### 3. Analyze Functions
\`\`\`bash
head -20 ${PREFIX}_functions_${TIMESTAMP}.txt
\`\`\`

## Quick Diagnosis

EOF

# Add quick analysis to summary
echo "### Top CPU Consumers" >> "${PREFIX}_summary_${TIMESTAMP}.md"
echo '```' >> "${PREFIX}_summary_${TIMESTAMP}.md"
head -10 "${PREFIX}_hotspots_${TIMESTAMP}.txt" >> "${PREFIX}_summary_${TIMESTAMP}.md" 2>/dev/null || echo "No hotspot data captured" >> "${PREFIX}_summary_${TIMESTAMP}.md"
echo '```' >> "${PREFIX}_summary_${TIMESTAMP}.md"

echo "" >> "${PREFIX}_summary_${TIMESTAMP}.md"
echo "### Resource Usage" >> "${PREFIX}_summary_${TIMESTAMP}.md"
echo '```' >> "${PREFIX}_summary_${TIMESTAMP}.md"
cat "${PREFIX}_resources_${TIMESTAMP}.txt" >> "${PREFIX}_summary_${TIMESTAMP}.md"
echo '```' >> "${PREFIX}_summary_${TIMESTAMP}.md"

# Final output
echo -e "${GREEN}✅ Profiling complete!${NC}"
echo -e "${BLUE}📁 Output directory: $(pwd)${NC}"
echo ""
echo -e "${YELLOW}📊 Generated files:${NC}"
ls -la ${PREFIX}_*_${TIMESTAMP}.* | while read line; do
    echo -e "  📄 $line"
done

echo ""
echo -e "${YELLOW}🚀 Quick Actions:${NC}"
echo -e "  📖 View summary: ${GREEN}cat ${PREFIX}_summary_${TIMESTAMP}.md${NC}"
echo -e "  🔥 Check hotspots: ${GREEN}cat ${PREFIX}_hotspots_${TIMESTAMP}.txt${NC}"
echo -e "  🌋 Open flame graph: ${GREEN}firefox ${PREFIX}_flamegraph_${TIMESTAMP}.svg${NC}"

echo ""
echo -e "${BLUE}💡 Tips:${NC}"
echo -e "  • Look for functions taking >10% CPU time"
echo -e "  • Check for blocking I/O operations"
echo -e "  • Identify memory allocation hotspots"
echo -e "  • Compare profiles before/after optimizations"

echo ""
echo -e "${GREEN}Happy profiling! 🔍${NC}" 