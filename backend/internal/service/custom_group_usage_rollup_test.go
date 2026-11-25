//go:build unit

package service

import (
	"os"
	"os/exec"
	"testing"
	"time"

	appTimezone "github.com/Wei-Shaw/sub2api/internal/pkg/timezone"
	"github.com/stretchr/testify/require"
)

func useGroupUsageTestTimezone(t *testing.T, name string) bool {
	t.Helper()

	// time.Local is process-global; other tests may have live background workers.
	if os.Getenv("NEXUS_GROUP_USAGE_TZ_CHILD") != name {
		cmd := exec.Command(os.Args[0], "-test.run=^"+t.Name()+"$")
		cmd.Env = append(os.Environ(), "NEXUS_GROUP_USAGE_TZ_CHILD="+name)
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, "%s", out)
		return false
	}
	require.NoError(t, appTimezone.Init(name))
	return true
}

func TestGroupUsageDateUsesConfiguredTimezoneBoundary(t *testing.T) {
	if !useGroupUsageTestTimezone(t, "America/New_York") {
		return
	}

	beforeMidnight := time.Date(2026, 3, 9, 3, 59, 59, 0, time.UTC)
	atMidnight := time.Date(2026, 3, 9, 4, 0, 0, 0, time.UTC)

	require.Equal(t, "2026-03-08", GroupUsageDate(beforeMidnight))
	require.Equal(t, "2026-03-09", GroupUsageDate(atMidnight))
	require.Equal(t, atMidnight, GroupUsageTodayStart(atMidnight))
}

func TestGroupUsageParseDateUsesConfiguredTimezone(t *testing.T) {
	if !useGroupUsageTestTimezone(t, "America/New_York") {
		return
	}

	parsed, err := ParseGroupUsageDate("2026-03-08")
	require.NoError(t, err)
	require.Equal(t, time.Date(2026, 3, 8, 5, 0, 0, 0, time.UTC), parsed.UTC())
	require.Equal(t, "America/New_York", parsed.Location().String())
}

func TestGroupUsageYesterdayStartHandlesDST(t *testing.T) {
	if !useGroupUsageTestTimezone(t, "America/New_York") {
		return
	}

	todayStart := time.Date(2026, 3, 9, 4, 0, 0, 0, time.UTC)
	yesterdayStart := GroupUsageYesterdayStart(todayStart)

	require.Equal(t, time.Date(2026, 3, 8, 5, 0, 0, 0, time.UTC), yesterdayStart)
	require.Equal(t, 23*time.Hour, todayStart.Sub(yesterdayStart))
	require.Equal(t, "America/New_York", GroupUsageTimezoneName())
}
