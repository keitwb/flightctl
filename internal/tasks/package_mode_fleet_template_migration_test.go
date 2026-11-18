package tasks

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/flightctl/flightctl/internal/domain"
	checkpointservice "github.com/flightctl/flightctl/internal/service/checkpoint"
	fleetservice "github.com/flightctl/flightctl/internal/service/fleet"
	"github.com/google/uuid"
	"github.com/samber/lo"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

// allowWrites permits writes without asserting on them, for tests whose subject
// is the walk rather than what it writes.
func allowWrites(fleetSvc *fleetservice.MockService) {
	fleetSvc.EXPECT().ReplaceFleet(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&domain.Fleet{}, domain.StatusOK()).AnyTimes()
}

func migrationTestConfig() PackageModeTemplateMigrationConfig {
	return PackageModeTemplateMigrationConfig{
		PageSize:   10,
		BatchSize:  10,
		TimeBudget: time.Minute,
	}
}

func testLogger() logrus.FieldLogger {
	log := logrus.New()
	log.SetLevel(logrus.PanicLevel)
	return log
}

// fakeCheckpoints is an in-memory checkpoint service that records every write,
// so a test can assert the durable state the next poll would read.
type fakeCheckpoints struct {
	values map[string][]byte
	writes int
	getErr *domain.Status
	setErr *domain.Status
}

func newFakeCheckpoints() *fakeCheckpoints {
	return &fakeCheckpoints{values: map[string][]byte{}}
}

func (f *fakeCheckpoints) GetCheckpoint(_ context.Context, consumer, key string) ([]byte, domain.Status) {
	if f.getErr != nil {
		return nil, *f.getErr
	}
	value, ok := f.values[consumer+"/"+key]
	if !ok {
		return nil, domain.StatusResourceNotFound("Checkpoint", key)
	}
	return value, domain.StatusOK()
}

func (f *fakeCheckpoints) SetCheckpoint(_ context.Context, consumer, key string, value []byte) domain.Status {
	if f.setErr != nil {
		return *f.setErr
	}
	f.writes++
	f.values[consumer+"/"+key] = value
	return domain.StatusOK()
}

func (f *fakeCheckpoints) GetDatabaseTime(context.Context) (time.Time, domain.Status) {
	return time.Now(), domain.StatusOK()
}

func (f *fakeCheckpoints) stored(t *testing.T, orgID uuid.UUID) packageModeMigrationCheckpoint {
	t.Helper()
	raw, ok := f.values[packageModeTemplateMigrationConsumer+"/"+packageModeMigrationCheckpointKey(orgID)]
	require.True(t, ok, "expected a checkpoint to have been written")
	var checkpoint packageModeMigrationCheckpoint
	require.NoError(t, json.Unmarshal(raw, &checkpoint))
	return checkpoint
}

func (f *fakeCheckpoints) seed(t *testing.T, orgID uuid.UUID, checkpoint packageModeMigrationCheckpoint) {
	t.Helper()
	encoded, err := json.Marshal(checkpoint)
	require.NoError(t, err)
	f.values[packageModeTemplateMigrationConsumer+"/"+packageModeMigrationCheckpointKey(orgID)] = encoded
}

var _ checkpointservice.Service = (*fakeCheckpoints)(nil)

func fleetWithImage(name, image string) domain.Fleet {
	fleet := domain.Fleet{
		Metadata: domain.ObjectMeta{
			Name:            lo.ToPtr(name),
			ResourceVersion: lo.ToPtr("1"),
		},
	}
	if image != "" {
		fleet.Spec.Template.Spec.Os = &domain.DeviceOsSpec{Image: image}
	}
	return fleet
}

func fleetList(continueToken *string, fleets ...domain.Fleet) *domain.FleetList {
	return &domain.FleetList{
		Items:    fleets,
		Metadata: domain.ListMeta{Continue: continueToken},
	}
}

func TestNewPackageModeFleetTemplateMigrationTaskValidatesDependencies(t *testing.T) {
	ctrl := gomock.NewController(t)
	fleetSvc := fleetservice.NewMockService(ctrl)
	checkpoints := newFakeCheckpoints()

	tests := []struct {
		name     string
		fleetSvc fleetservice.Service
		cps      checkpointservice.Service
		config   PackageModeTemplateMigrationConfig
	}{
		{name: "When the fleet service is missing it should be rejected", fleetSvc: nil, cps: checkpoints, config: migrationTestConfig()},
		{name: "When the checkpoint service is missing it should be rejected", fleetSvc: fleetSvc, cps: nil, config: migrationTestConfig()},
		{
			name: "When the page size is below one it should be rejected", fleetSvc: fleetSvc, cps: checkpoints,
			config: PackageModeTemplateMigrationConfig{PageSize: 0, BatchSize: 1, TimeBudget: time.Minute},
		},
		{
			name: "When the batch size is below one it should be rejected", fleetSvc: fleetSvc, cps: checkpoints,
			config: PackageModeTemplateMigrationConfig{PageSize: 1, BatchSize: 0, TimeBudget: time.Minute},
		},
		{
			name: "When the time budget is not positive it should be rejected", fleetSvc: fleetSvc, cps: checkpoints,
			config: PackageModeTemplateMigrationConfig{PageSize: 1, BatchSize: 1, TimeBudget: 0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewPackageModeFleetTemplateMigrationTask(tt.fleetSvc, tt.cps, tt.config, testLogger())
			require.Error(t, err)
		})
	}

	t.Run("When every dependency is supplied it should be accepted", func(t *testing.T) {
		task, err := NewPackageModeFleetTemplateMigrationTask(fleetSvc, checkpoints, migrationTestConfig(), testLogger())
		require.NoError(t, err)
		require.NotNil(t, task)
	})
}

func TestMigrationLifecycleReachesCompleteAndThenStops(t *testing.T) {
	ctrl := gomock.NewController(t)
	fleetSvc := fleetservice.NewMockService(ctrl)
	checkpoints := newFakeCheckpoints()
	orgID := uuid.New()

	allowWrites(fleetSvc)
	// One page, exhausted immediately: the run should complete.
	fleetSvc.EXPECT().ListFleets(gomock.Any(), orgID, gomock.Any()).
		Return(fleetList(nil, fleetWithImage("a", "quay.io/org/os:v1")), domain.StatusOK()).Times(1)

	task, err := NewPackageModeFleetTemplateMigrationTask(fleetSvc, checkpoints, migrationTestConfig(), testLogger())
	require.NoError(t, err)

	task.Poll(t.Context(), orgID)
	require.Equal(t, packageModeMigrationComplete, checkpoints.stored(t, orgID).Phase)

	// A completed run must not list anything again; the mock would fail on an
	// unexpected second call.
	task.Poll(t.Context(), orgID)
	require.Equal(t, packageModeMigrationComplete, checkpoints.stored(t, orgID).Phase)
}

func TestMigrationAdvancesCursorAcrossPages(t *testing.T) {
	ctrl := gomock.NewController(t)
	fleetSvc := fleetservice.NewMockService(ctrl)
	checkpoints := newFakeCheckpoints()
	orgID := uuid.New()

	allowWrites(fleetSvc)
	gomock.InOrder(
		fleetSvc.EXPECT().ListFleets(gomock.Any(), orgID, gomock.Any()).
			DoAndReturn(func(_ context.Context, _ uuid.UUID, params domain.ListFleetsParams) (*domain.FleetList, domain.Status) {
				require.Nil(t, params.Continue, "the first page must start with no cursor")
				return fleetList(lo.ToPtr("page2"), fleetWithImage("a", "quay.io/org/os:v1")), domain.StatusOK()
			}),
		fleetSvc.EXPECT().ListFleets(gomock.Any(), orgID, gomock.Any()).
			DoAndReturn(func(_ context.Context, _ uuid.UUID, params domain.ListFleetsParams) (*domain.FleetList, domain.Status) {
				require.Equal(t, "page2", lo.FromPtr(params.Continue))
				return fleetList(nil, fleetWithImage("b", "quay.io/org/os:v2")), domain.StatusOK()
			}),
	)

	task, err := NewPackageModeFleetTemplateMigrationTask(fleetSvc, checkpoints, migrationTestConfig(), testLogger())
	require.NoError(t, err)
	task.Poll(t.Context(), orgID)

	stored := checkpoints.stored(t, orgID)
	require.Equal(t, packageModeMigrationComplete, stored.Phase)
	require.Nil(t, stored.Cursor)
}

func TestMigrationAbortsWhenTheCursorDoesNotAdvance(t *testing.T) {
	ctrl := gomock.NewController(t)
	fleetSvc := fleetservice.NewMockService(ctrl)
	checkpoints := newFakeCheckpoints()
	orgID := uuid.New()
	checkpoints.seed(t, orgID, packageModeMigrationCheckpoint{
		Version: packageModeTemplateMigrationVersion,
		Phase:   packageModeMigrationRunning,
		Cursor:  lo.ToPtr("stuck"),
	})

	allowWrites(fleetSvc)
	fleetSvc.EXPECT().ListFleets(gomock.Any(), orgID, gomock.Any()).
		Return(fleetList(lo.ToPtr("stuck"), fleetWithImage("a", "quay.io/org/os:v1")), domain.StatusOK()).Times(1)

	task, err := NewPackageModeFleetTemplateMigrationTask(fleetSvc, checkpoints, migrationTestConfig(), testLogger())
	require.NoError(t, err)
	task.Poll(t.Context(), orgID)

	stored := checkpoints.stored(t, orgID)
	require.NotEqual(t, packageModeMigrationComplete, stored.Phase,
		"a listing that cannot advance must not be reported as a finished run")
}

func TestMigrationDiscardsCheckpointFromAnotherSchemaVersion(t *testing.T) {
	ctrl := gomock.NewController(t)
	fleetSvc := fleetservice.NewMockService(ctrl)
	checkpoints := newFakeCheckpoints()
	orgID := uuid.New()

	allowWrites(fleetSvc)
	// A completed checkpoint written under a different schema version must not
	// suppress this version's run.
	checkpoints.seed(t, orgID, packageModeMigrationCheckpoint{
		Version: packageModeTemplateMigrationVersion - 1,
		Phase:   packageModeMigrationComplete,
	})

	fleetSvc.EXPECT().ListFleets(gomock.Any(), orgID, gomock.Any()).
		Return(fleetList(nil, fleetWithImage("a", "quay.io/org/os:v1")), domain.StatusOK()).Times(1)

	task, err := NewPackageModeFleetTemplateMigrationTask(fleetSvc, checkpoints, migrationTestConfig(), testLogger())
	require.NoError(t, err)
	task.Poll(t.Context(), orgID)

	require.Equal(t, packageModeMigrationComplete, checkpoints.stored(t, orgID).Phase)
}

func TestMigrationStopsAtTheTimeBudgetWithoutCompleting(t *testing.T) {
	ctrl := gomock.NewController(t)
	fleetSvc := fleetservice.NewMockService(ctrl)
	checkpoints := newFakeCheckpoints()
	orgID := uuid.New()

	config := migrationTestConfig()
	config.TimeBudget = time.Nanosecond

	task, err := NewPackageModeFleetTemplateMigrationTask(fleetSvc, checkpoints, config, testLogger())
	require.NoError(t, err)
	time.Sleep(time.Millisecond)
	task.Poll(t.Context(), orgID)

	stored := checkpoints.stored(t, orgID)
	require.Equal(t, packageModeMigrationRunning, stored.Phase,
		"a poll cut short by its budget has not proven the listing is exhausted")
}

func TestMigrationDoesNotCompleteWhenCancelled(t *testing.T) {
	ctrl := gomock.NewController(t)
	fleetSvc := fleetservice.NewMockService(ctrl)
	checkpoints := newFakeCheckpoints()
	orgID := uuid.New()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	task, err := NewPackageModeFleetTemplateMigrationTask(fleetSvc, checkpoints, migrationTestConfig(), testLogger())
	require.NoError(t, err)
	task.Poll(ctx, orgID)

	if stored, ok := checkpoints.values[packageModeTemplateMigrationConsumer+"/"+packageModeMigrationCheckpointKey(orgID)]; ok {
		var checkpoint packageModeMigrationCheckpoint
		require.NoError(t, json.Unmarshal(stored, &checkpoint))
		require.NotEqual(t, packageModeMigrationComplete, checkpoint.Phase)
	}
}

func TestMigrationKeepsARetryDurableWhenItsFleetCannotBeRead(t *testing.T) {
	ctrl := gomock.NewController(t)
	fleetSvc := fleetservice.NewMockService(ctrl)
	checkpoints := newFakeCheckpoints()
	orgID := uuid.New()

	checkpoints.seed(t, orgID, packageModeMigrationCheckpoint{
		Version: packageModeTemplateMigrationVersion,
		Phase:   packageModeMigrationRunning,
		Retry:   []string{"a"},
	})

	// The retry is attempted first, by name, and the read fails.
	fleetSvc.EXPECT().GetFleet(gomock.Any(), orgID, "a", gomock.Any()).
		Return(nil, domain.Status{Code: http.StatusInternalServerError, Message: "boom"}).Times(1)
	fleetSvc.EXPECT().ListFleets(gomock.Any(), orgID, gomock.Any()).
		Return(fleetList(nil), domain.StatusOK()).Times(1)

	task, err := NewPackageModeFleetTemplateMigrationTask(fleetSvc, checkpoints, migrationTestConfig(), testLogger())
	require.NoError(t, err)
	task.Poll(t.Context(), orgID)

	stored := checkpoints.stored(t, orgID)
	require.Equal(t, []string{"a"}, stored.Retry,
		"a fleet that could not be read still owes a retry on the next poll")
	require.NotEqual(t, packageModeMigrationComplete, stored.Phase,
		"a run that still owes a retry must not report itself finished")
}

func TestMigrationStopsWhenTheCheckpointCannotBeRead(t *testing.T) {
	ctrl := gomock.NewController(t)
	// No fleet service call is expected: an unreadable checkpoint means the poll
	// does not know what it has already done, so it must not walk fleets.
	fleetSvc := fleetservice.NewMockService(ctrl)
	checkpoints := newFakeCheckpoints()
	checkpoints.getErr = &domain.Status{Code: http.StatusInternalServerError, Message: "boom"}
	orgID := uuid.New()

	task, err := NewPackageModeFleetTemplateMigrationTask(fleetSvc, checkpoints, migrationTestConfig(), testLogger())
	require.NoError(t, err)
	task.Poll(t.Context(), orgID)

	require.Zero(t, checkpoints.writes,
		"a poll that cannot read its checkpoint must not overwrite it either")
}

func TestMigrationLeavesNoCheckpointWhenItCannotBeWritten(t *testing.T) {
	ctrl := gomock.NewController(t)
	fleetSvc := fleetservice.NewMockService(ctrl)
	checkpoints := newFakeCheckpoints()
	checkpoints.setErr = &domain.Status{Code: http.StatusInternalServerError, Message: "boom"}
	orgID := uuid.New()

	allowWrites(fleetSvc)
	fleetSvc.EXPECT().ListFleets(gomock.Any(), orgID, gomock.Any()).
		Return(fleetList(nil, fleetWithImage("a", "quay.io/org/os:v1")), domain.StatusOK()).Times(1)

	task, err := NewPackageModeFleetTemplateMigrationTask(fleetSvc, checkpoints, migrationTestConfig(), testLogger())
	require.NoError(t, err)
	task.Poll(t.Context(), orgID)

	_, ok := checkpoints.values[packageModeTemplateMigrationConsumer+"/"+packageModeMigrationCheckpointKey(orgID)]
	require.False(t, ok, "a checkpoint write that failed must not look like it succeeded")
}

func TestMigrationKeepsRetriesItCouldNotAttempt(t *testing.T) {
	// A bound can end a poll before the fleets owing a retry have been worked
	// through. Those left over still owe a retry on the next poll.
	seeded := packageModeMigrationCheckpoint{
		Version: packageModeTemplateMigrationVersion,
		Phase:   packageModeMigrationRunning,
		Retry:   []string{"a", "b"},
	}

	t.Run("When the time budget stops the drain it should keep them", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		// No fleet service call is expected: the budget is spent on arrival.
		fleetSvc := fleetservice.NewMockService(ctrl)
		checkpoints := newFakeCheckpoints()
		orgID := uuid.New()
		checkpoints.seed(t, orgID, seeded)

		config := migrationTestConfig()
		config.TimeBudget = time.Nanosecond

		task, err := NewPackageModeFleetTemplateMigrationTask(fleetSvc, checkpoints, config, testLogger())
		require.NoError(t, err)
		task.Poll(t.Context(), orgID)

		stored := checkpoints.stored(t, orgID)
		require.Equal(t, []string{"a", "b"}, stored.Retry)
		require.NotEqual(t, packageModeMigrationComplete, stored.Phase)
	})

	t.Run("When the poll is cancelled it should keep them", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		fleetSvc := fleetservice.NewMockService(ctrl)
		checkpoints := newFakeCheckpoints()
		orgID := uuid.New()
		checkpoints.seed(t, orgID, seeded)

		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		task, err := NewPackageModeFleetTemplateMigrationTask(fleetSvc, checkpoints, migrationTestConfig(), testLogger())
		require.NoError(t, err)
		task.Poll(ctx, orgID)

		stored := checkpoints.stored(t, orgID)
		require.Equal(t, []string{"a", "b"}, stored.Retry)
		require.NotEqual(t, packageModeMigrationComplete, stored.Phase)
	})
}

func TestMigrationStopsRetryingAFleetThatIsGone(t *testing.T) {
	ctrl := gomock.NewController(t)
	fleetSvc := fleetservice.NewMockService(ctrl)
	checkpoints := newFakeCheckpoints()
	orgID := uuid.New()

	checkpoints.seed(t, orgID, packageModeMigrationCheckpoint{
		Version: packageModeTemplateMigrationVersion,
		Phase:   packageModeMigrationRunning,
		Retry:   []string{"a"},
	})

	// The fleet was deleted between the two polls, so there is nothing left to
	// migrate for it and it must not hold the run open forever.
	fleetSvc.EXPECT().GetFleet(gomock.Any(), orgID, "a", gomock.Any()).
		Return(nil, domain.StatusResourceNotFound("Fleet", "a")).Times(1)
	fleetSvc.EXPECT().ListFleets(gomock.Any(), orgID, gomock.Any()).
		Return(fleetList(nil), domain.StatusOK()).Times(1)

	task, err := NewPackageModeFleetTemplateMigrationTask(fleetSvc, checkpoints, migrationTestConfig(), testLogger())
	require.NoError(t, err)
	task.Poll(t.Context(), orgID)

	stored := checkpoints.stored(t, orgID)
	require.Empty(t, stored.Retry, "a fleet that no longer exists stops owing a retry")
	require.Equal(t, packageModeMigrationComplete, stored.Phase)
}

func TestMigrationRecordsADeferredFleetOnlyOnce(t *testing.T) {
	ctrl := gomock.NewController(t)
	fleetSvc := fleetservice.NewMockService(ctrl)
	checkpoints := newFakeCheckpoints()
	orgID := uuid.New()

	// The fleet was already reported to its owner by an earlier poll. Seeing it
	// again must not grow the checkpoint.
	checkpoints.seed(t, orgID, packageModeMigrationCheckpoint{
		Version:  packageModeTemplateMigrationVersion,
		Phase:    packageModeMigrationRunning,
		Deferred: []deferredFleet{{Name: "from-git", Owner: "ResourceSync/the-sync"}},
	})

	owned := fleetWithImage("from-git", "quay.io/org/os:v2")
	owned.Metadata.Owner = lo.ToPtr("ResourceSync/the-sync")
	fleetSvc.EXPECT().ListFleets(gomock.Any(), orgID, gomock.Any()).
		Return(fleetList(nil, owned), domain.StatusOK()).Times(1)

	task, err := NewPackageModeFleetTemplateMigrationTask(fleetSvc, checkpoints, migrationTestConfig(), testLogger())
	require.NoError(t, err)
	task.Poll(t.Context(), orgID)

	stored := checkpoints.stored(t, orgID)
	require.Len(t, stored.Deferred, 1, "a deferred fleet is recorded once, not once per poll")
	require.Equal(t, "ResourceSync/the-sync", stored.Deferred[0].Owner)
}

func TestGuardedOsImagePreservesTheOriginalValue(t *testing.T) {
	tests := []struct {
		name  string
		image string
		want  string
	}{
		{
			name:  "When the image is a literal it should be wrapped unchanged",
			image: "quay.io/org/os:v2",
			want:  "{{ if deviceHasImageMode }}quay.io/org/os:v2{{ end }}",
		},
		{
			name:  "When the image uses a registry port it should keep the port",
			image: "quay.io:5000/org/os:v2",
			want:  "{{ if deviceHasImageMode }}quay.io:5000/org/os:v2{{ end }}",
		},
		{
			name:  "When the image uses a digest it should keep the digest",
			image: "quay.io/org/os@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			want:  "{{ if deviceHasImageMode }}quay.io/org/os@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef{{ end }}",
		},
		{
			name:  "When the tag is parameterized it should keep the substitution",
			image: "quay.io/org/os:{{ .metadata.labels.version }}",
			want:  "{{ if deviceHasImageMode }}quay.io/org/os:{{ .metadata.labels.version }}{{ end }}",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, guardedOsImage(tt.image))
		})
	}
}

func TestMigrationWritesGuardedImageAndPreservesEverythingElse(t *testing.T) {
	ctrl := gomock.NewController(t)
	fleetSvc := fleetservice.NewMockService(ctrl)
	checkpoints := newFakeCheckpoints()
	orgID := uuid.New()

	catalogRef := domain.CatalogItemRefSpec{Catalog: "example-catalog", Item: "example-item", Version: "1.0.0"}

	// A catalog item reference travels on an application, not beside the OS
	// image: the API rejects a device OS that names both. The fleet still has to
	// come back with the reference untouched.
	original := fleetWithImage("a", "quay.io/org/os:v2")
	original.Metadata.ResourceVersion = lo.ToPtr("7")
	original.Metadata.Labels = &map[string]string{"team": "edge"}
	original.Spec.Template.Spec.Applications = lo.ToPtr([]domain.ApplicationProviderSpec{
		catalogRefApplication(t, "catalog-app", catalogRef),
	})

	fleetSvc.EXPECT().ListFleets(gomock.Any(), orgID, gomock.Any()).
		Return(fleetList(nil, original), domain.StatusOK()).Times(1)
	fleetSvc.EXPECT().ReplaceFleet(gomock.Any(), orgID, "a", gomock.Any(), false).
		DoAndReturn(func(_ context.Context, _ uuid.UUID, _ string, fleet domain.Fleet, _ bool) (*domain.Fleet, domain.Status) {
			require.Equal(t, "{{ if deviceHasImageMode }}quay.io/org/os:v2{{ end }}", fleet.Spec.Template.Spec.Os.Image)
			require.Equal(t, "7", lo.FromPtr(fleet.Metadata.ResourceVersion),
				"the resource version that was read must go back with the write")
			apps := lo.FromPtr(fleet.Spec.Template.Spec.Applications)
			require.Len(t, apps, 1)
			require.Equal(t, catalogRef, applicationCatalogRef(t, apps[0]),
				"a catalog item reference must survive the migration unchanged")
			require.Equal(t, map[string]string{"team": "edge"}, lo.FromPtr(fleet.Metadata.Labels))
			return &fleet, domain.StatusOK()
		}).Times(1)

	task, err := NewPackageModeFleetTemplateMigrationTask(fleetSvc, checkpoints, migrationTestConfig(), testLogger())
	require.NoError(t, err)
	task.Poll(t.Context(), orgID)

	stored := checkpoints.stored(t, orgID)
	require.Equal(t, packageModeMigrationComplete, stored.Phase)
	require.Equal(t, 1, stored.Transformed)
}

func TestMigrationSecondRunMakesNoWrites(t *testing.T) {
	ctrl := gomock.NewController(t)
	fleetSvc := fleetservice.NewMockService(ctrl)
	checkpoints := newFakeCheckpoints()
	orgID := uuid.New()

	// Already guarded: the fleet is skipped, so no write is attempted at all.
	fleetSvc.EXPECT().ListFleets(gomock.Any(), orgID, gomock.Any()).
		Return(fleetList(nil, fleetWithImage("a", "{{ if deviceHasImageMode }}quay.io/org/os:v2{{ end }}")), domain.StatusOK()).Times(1)

	task, err := NewPackageModeFleetTemplateMigrationTask(fleetSvc, checkpoints, migrationTestConfig(), testLogger())
	require.NoError(t, err)
	task.Poll(t.Context(), orgID)

	stored := checkpoints.stored(t, orgID)
	require.Equal(t, packageModeMigrationComplete, stored.Phase)
	require.Equal(t, 0, stored.Transformed)
}

func TestMigrationDeferredResourceSyncFleetIsNeverWritten(t *testing.T) {
	ctrl := gomock.NewController(t)
	fleetSvc := fleetservice.NewMockService(ctrl)
	checkpoints := newFakeCheckpoints()
	orgID := uuid.New()

	owned := fleetWithImage("from-git", "quay.io/org/os:v2")
	owned.Metadata.Owner = lo.ToPtr("ResourceSync/the-sync")
	fleetSvc.EXPECT().ListFleets(gomock.Any(), orgID, gomock.Any()).
		Return(fleetList(nil, owned), domain.StatusOK()).Times(1)
	// No ReplaceFleet expectation: a write would fail the test.

	task, err := NewPackageModeFleetTemplateMigrationTask(fleetSvc, checkpoints, migrationTestConfig(), testLogger())
	require.NoError(t, err)
	task.Poll(t.Context(), orgID)

	stored := checkpoints.stored(t, orgID)
	require.Len(t, stored.Deferred, 1)
	require.Equal(t, "from-git", stored.Deferred[0].Name)
	require.Equal(t, "ResourceSync/the-sync", stored.Deferred[0].Owner)
}

func TestMigrationRetriesAConflictOnTheNextPollAfterABudgetStop(t *testing.T) {
	ctrl := gomock.NewController(t)
	fleetSvc := fleetservice.NewMockService(ctrl)
	checkpoints := newFakeCheckpoints()
	orgID := uuid.New()

	// One write per poll, and the listing has more pages after this one, so the
	// first poll stops on its batch budget long before the listing is exhausted.
	config := migrationTestConfig()
	config.BatchSize = 1

	conflicted := fleetWithImage("a", "quay.io/org/os:v2")
	conflicted.Metadata.ResourceVersion = lo.ToPtr("1")

	gomock.InOrder(
		fleetSvc.EXPECT().ListFleets(gomock.Any(), orgID, gomock.Any()).
			Return(fleetList(lo.ToPtr("page2"), conflicted, fleetWithImage("b", "quay.io/org/os:v3")), domain.StatusOK()).Times(1),
		fleetSvc.EXPECT().ReplaceFleet(gomock.Any(), orgID, "a", gomock.Any(), false).
			DoAndReturn(func(_ context.Context, _ uuid.UUID, _ string, fleet domain.Fleet, _ bool) (*domain.Fleet, domain.Status) {
				require.Equal(t, "1", lo.FromPtr(fleet.Metadata.ResourceVersion))
				return nil, domain.Status{Code: 409, Message: "conflict"}
			}).Times(1),
	)

	task, err := NewPackageModeFleetTemplateMigrationTask(fleetSvc, checkpoints, config, testLogger())
	require.NoError(t, err)
	task.Poll(t.Context(), orgID)

	stored := checkpoints.stored(t, orgID)
	require.Equal(t, []string{"a"}, stored.Retry,
		"a conflicted fleet must be durable, not held only in memory")
	require.NotEqual(t, packageModeMigrationComplete, stored.Phase,
		"a run with a conflict outstanding must not report itself finished")

	// The next poll retries the conflicted fleet first, by name, with a value
	// read fresh rather than the stale one from the previous listing.
	refreshed := fleetWithImage("a", "quay.io/org/os:v2")
	refreshed.Metadata.ResourceVersion = lo.ToPtr("9")
	gomock.InOrder(
		fleetSvc.EXPECT().GetFleet(gomock.Any(), orgID, "a", gomock.Any()).
			Return(&refreshed, domain.StatusOK()).Times(1),
		fleetSvc.EXPECT().ReplaceFleet(gomock.Any(), orgID, "a", gomock.Any(), false).
			DoAndReturn(func(_ context.Context, _ uuid.UUID, _ string, fleet domain.Fleet, _ bool) (*domain.Fleet, domain.Status) {
				require.Equal(t, "9", lo.FromPtr(fleet.Metadata.ResourceVersion),
					"the retry must use the freshly read resource version")
				require.Equal(t, "{{ if deviceHasImageMode }}quay.io/org/os:v2{{ end }}", fleet.Spec.Template.Spec.Os.Image)
				return &fleet, domain.StatusOK()
			}).Times(1),
	)

	task.Poll(t.Context(), orgID)
	require.Empty(t, checkpoints.stored(t, orgID).Retry)
}

func TestMigrationDoesNotLosePageRemainderAtTheBatchLimit(t *testing.T) {
	ctrl := gomock.NewController(t)
	fleetSvc := fleetservice.NewMockService(ctrl)
	checkpoints := newFakeCheckpoints()
	orgID := uuid.New()

	config := migrationTestConfig()
	config.BatchSize = 1

	fleetSvc.EXPECT().ListFleets(gomock.Any(), orgID, gomock.Any()).
		DoAndReturn(func(_ context.Context, _ uuid.UUID, params domain.ListFleetsParams) (*domain.FleetList, domain.Status) {
			require.Nil(t, params.Continue)
			return fleetList(lo.ToPtr("page2"),
				fleetWithImage("a", "quay.io/org/os:v1"),
				fleetWithImage("b", "quay.io/org/os:v2")), domain.StatusOK()
		}).Times(1)
	fleetSvc.EXPECT().ReplaceFleet(gomock.Any(), orgID, "a", gomock.Any(), false).
		Return(&domain.Fleet{}, domain.StatusOK()).Times(1)

	task, err := NewPackageModeFleetTemplateMigrationTask(fleetSvc, checkpoints, config, testLogger())
	require.NoError(t, err)
	task.Poll(t.Context(), orgID)

	stored := checkpoints.stored(t, orgID)
	require.Nil(t, stored.Cursor,
		"stopping inside a page must leave the cursor at that page so its remainder is listed again")
	require.NotEqual(t, packageModeMigrationComplete, stored.Phase)
}

func TestMigrationRestartsAfterAScaffoldOnlyCompletion(t *testing.T) {
	ctrl := gomock.NewController(t)
	fleetSvc := fleetservice.NewMockService(ctrl)
	checkpoints := newFakeCheckpoints()
	orgID := uuid.New()

	// A run completed by the scaffold-only build wrote version 1 and performed
	// no transformation. Now that the migration writes, that completion must
	// not suppress the work.
	checkpoints.seed(t, orgID, packageModeMigrationCheckpoint{
		Version: 1,
		Phase:   packageModeMigrationComplete,
	})
	require.Greater(t, packageModeTemplateMigrationVersion, 1,
		"the checkpoint version must advance when the task starts writing")

	fleetSvc.EXPECT().ListFleets(gomock.Any(), orgID, gomock.Any()).
		Return(fleetList(nil, fleetWithImage("a", "quay.io/org/os:v2")), domain.StatusOK()).Times(1)
	fleetSvc.EXPECT().ReplaceFleet(gomock.Any(), orgID, "a", gomock.Any(), false).
		Return(&domain.Fleet{}, domain.StatusOK()).Times(1)

	task, err := NewPackageModeFleetTemplateMigrationTask(fleetSvc, checkpoints, migrationTestConfig(), testLogger())
	require.NoError(t, err)
	task.Poll(t.Context(), orgID)

	stored := checkpoints.stored(t, orgID)
	require.Equal(t, packageModeTemplateMigrationVersion, stored.Version)
	require.Equal(t, 1, stored.Transformed)
}

func TestMigrationDryRunWritesNothing(t *testing.T) {
	ctrl := gomock.NewController(t)
	fleetSvc := fleetservice.NewMockService(ctrl)
	checkpoints := newFakeCheckpoints()
	orgID := uuid.New()

	config := migrationTestConfig()
	config.DryRun = true

	fleetSvc.EXPECT().ListFleets(gomock.Any(), orgID, gomock.Any()).
		Return(fleetList(nil, fleetWithImage("a", "quay.io/org/os:v2")), domain.StatusOK()).Times(1)

	task, err := NewPackageModeFleetTemplateMigrationTask(fleetSvc, checkpoints, config, testLogger())
	require.NoError(t, err)
	task.Poll(t.Context(), orgID)

	require.Equal(t, 0, checkpoints.stored(t, orgID).Transformed)
}

// catalogRefApplication builds a container application that takes its image
// from a catalog item, the shape in which a fleet template carries a catalog
// item reference alongside an OS image.
func catalogRefApplication(t *testing.T, name string, ref domain.CatalogItemRefSpec) domain.ApplicationProviderSpec {
	t.Helper()
	container := domain.ContainerApplication{AppType: domain.AppTypeContainer, Name: lo.ToPtr(name)}
	require.NoError(t, container.FromCatalogItemRefApplicationProviderSpec(
		domain.CatalogItemRefApplicationProviderSpec{CatalogItemRef: ref}))
	var app domain.ApplicationProviderSpec
	require.NoError(t, app.FromContainerApplication(container))
	return app
}

// applicationCatalogRef reads the catalog item reference back out of an
// application provider spec.
func applicationCatalogRef(t *testing.T, app domain.ApplicationProviderSpec) domain.CatalogItemRefSpec {
	t.Helper()
	container, err := app.AsContainerApplication()
	require.NoError(t, err)
	provider, err := container.AsCatalogItemRefApplicationProviderSpec()
	require.NoError(t, err)
	return provider.CatalogItemRef
}

func TestMigrationReplaysTheListingWhenTheRetryListIsFull(t *testing.T) {
	ctrl := gomock.NewController(t)
	fleetSvc := fleetservice.NewMockService(ctrl)
	checkpoints := newFakeCheckpoints()
	orgID := uuid.New()

	// More fleets conflict in one page than the retry list can name, and the
	// listing has a further page. The fleets that could not be named must not be
	// left behind the cursor.
	overflow := maxPackageModeTemplateMigrationRetries + 1
	config := migrationTestConfig()
	config.PageSize = int32(overflow)
	config.BatchSize = overflow

	fleets := make([]domain.Fleet, 0, overflow)
	for i := 0; i < overflow; i++ {
		fleets = append(fleets, fleetWithImage(fmt.Sprintf("fleet-%03d", i), "quay.io/org/os:v2"))
	}

	fleetSvc.EXPECT().ListFleets(gomock.Any(), orgID, gomock.Any()).
		Return(fleetList(lo.ToPtr("page2"), fleets...), domain.StatusOK()).Times(1)
	fleetSvc.EXPECT().ReplaceFleet(gomock.Any(), orgID, gomock.Any(), gomock.Any(), false).
		Return(nil, domain.Status{Code: http.StatusConflict, Message: "conflict"}).Times(overflow)

	task, err := NewPackageModeFleetTemplateMigrationTask(fleetSvc, checkpoints, config, testLogger())
	require.NoError(t, err)
	task.Poll(t.Context(), orgID)

	stored := checkpoints.stored(t, orgID)
	require.Len(t, stored.Retry, maxPackageModeTemplateMigrationRetries,
		"the retry list stays bounded")
	require.Nil(t, stored.Cursor,
		"a fleet that could not be named for retry is only found again by replaying the listing")
	require.NotEqual(t, packageModeMigrationComplete, stored.Phase)
}
