package store_test

import (
	"context"
	"time"

	"github.com/flightctl/flightctl/internal/config"
	"github.com/flightctl/flightctl/internal/flterrors"
	"github.com/flightctl/flightctl/internal/store"
	"github.com/flightctl/flightctl/internal/store/model"
	flightlog "github.com/flightctl/flightctl/pkg/log"
	testutil "github.com/flightctl/flightctl/test/util"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"
)

var _ = Describe("SyncStateStore", func() {
	var (
		log       *logrus.Logger
		ctx       context.Context
		orgId     uuid.UUID
		storeInst store.Store
		cfg       *config.Config
		dbName    string
	)

	BeforeEach(func() {
		ctx = testutil.StartSpecTracerForGinkgo(suiteCtx)
		log = flightlog.InitLogs()
		storeInst, cfg, dbName, _ = store.PrepareDBForUnitTests(ctx, log)

		orgId = uuid.New()
		err := testutil.CreateTestOrganization(ctx, storeInst, orgId)
		Expect(err).ToNot(HaveOccurred())
	})

	AfterEach(func() {
		store.DeleteTestDB(ctx, log, cfg, storeInst, dbName)
	})

	Context("When setting and getting sync state", func() {
		It("should return the stored state for a valid key", func() {
			now := time.Now().UTC().Truncate(time.Microsecond)
			state := &model.SyncState{
				OrgID:         orgId,
				ResourceKey:   "git:my-repo/main",
				Fingerprint:   "abc123def456",
				LastCheckedAt: now,
				LastChangeAt:  &now,
			}
			err := storeInst.SyncState().Set(ctx, orgId, state)
			Expect(err).ToNot(HaveOccurred())

			got, err := storeInst.SyncState().Get(ctx, orgId, "git:my-repo/main")
			Expect(err).ToNot(HaveOccurred())
			Expect(got.ResourceKey).To(Equal("git:my-repo/main"))
			Expect(got.Fingerprint).To(Equal("abc123def456"))
			Expect(got.OrgID).To(Equal(orgId))
			Expect(got.LastCheckedAt.UTC()).To(BeTemporally("~", now, time.Millisecond))
			Expect(got.LastChangeAt).ToNot(BeNil())
			Expect(got.LastChangeAt.UTC()).To(BeTemporally("~", now, time.Millisecond))
		})

		It("should upsert when setting an existing key", func() {
			now := time.Now().UTC().Truncate(time.Microsecond)
			state := &model.SyncState{
				OrgID:         orgId,
				ResourceKey:   "git:my-repo/main",
				Fingerprint:   "old-fingerprint",
				LastCheckedAt: now,
			}
			err := storeInst.SyncState().Set(ctx, orgId, state)
			Expect(err).ToNot(HaveOccurred())

			later := now.Add(5 * time.Minute)
			state.Fingerprint = "new-fingerprint"
			state.LastCheckedAt = later
			state.LastChangeAt = &later
			err = storeInst.SyncState().Set(ctx, orgId, state)
			Expect(err).ToNot(HaveOccurred())

			got, err := storeInst.SyncState().Get(ctx, orgId, "git:my-repo/main")
			Expect(err).ToNot(HaveOccurred())
			Expect(got.Fingerprint).To(Equal("new-fingerprint"))
			Expect(got.LastCheckedAt.UTC()).To(BeTemporally("~", later, time.Millisecond))
			Expect(got.LastChangeAt).ToNot(BeNil())
			Expect(got.LastChangeAt.UTC()).To(BeTemporally("~", later, time.Millisecond))
		})

		It("should return ErrResourceNotFound for a non-existent key", func() {
			_, err := storeInst.SyncState().Get(ctx, orgId, "git:nonexistent/main")
			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError(flterrors.ErrResourceNotFound))
		})

		It("should allow LastChangeAt to be nil", func() {
			now := time.Now().UTC().Truncate(time.Microsecond)
			state := &model.SyncState{
				OrgID:         orgId,
				ResourceKey:   "http:my-http-repo/status.json",
				Fingerprint:   "etag-abc",
				LastCheckedAt: now,
				LastChangeAt:  nil,
			}
			err := storeInst.SyncState().Set(ctx, orgId, state)
			Expect(err).ToNot(HaveOccurred())

			got, err := storeInst.SyncState().Get(ctx, orgId, "http:my-http-repo/status.json")
			Expect(err).ToNot(HaveOccurred())
			Expect(got.LastChangeAt).To(BeNil())
		})
	})

	Context("When listing sync states", func() {
		It("should return empty slice when no entries exist", func() {
			states, err := storeInst.SyncState().List(ctx, orgId)
			Expect(err).ToNot(HaveOccurred())
			Expect(states).To(BeEmpty())
		})

		It("should return all entries for the org", func() {
			now := time.Now().UTC().Truncate(time.Microsecond)
			keys := []string{
				"git:repo-a/main",
				"git:repo-b/develop",
				"secret:default/my-secret",
			}
			for _, key := range keys {
				err := storeInst.SyncState().Set(ctx, orgId, &model.SyncState{
					OrgID:         orgId,
					ResourceKey:   key,
					Fingerprint:   "fp-" + key,
					LastCheckedAt: now,
				})
				Expect(err).ToNot(HaveOccurred())
			}

			states, err := storeInst.SyncState().List(ctx, orgId)
			Expect(err).ToNot(HaveOccurred())
			Expect(states).To(HaveLen(3))
		})
	})

	Context("When enforcing org isolation", func() {
		It("should not return entries from a different org", func() {
			otherOrgId := uuid.New()
			err := testutil.CreateTestOrganization(ctx, storeInst, otherOrgId)
			Expect(err).ToNot(HaveOccurred())

			now := time.Now().UTC().Truncate(time.Microsecond)
			err = storeInst.SyncState().Set(ctx, orgId, &model.SyncState{
				OrgID:         orgId,
				ResourceKey:   "git:shared-name/main",
				Fingerprint:   "org-a-fingerprint",
				LastCheckedAt: now,
			})
			Expect(err).ToNot(HaveOccurred())

			err = storeInst.SyncState().Set(ctx, otherOrgId, &model.SyncState{
				OrgID:         otherOrgId,
				ResourceKey:   "git:shared-name/main",
				Fingerprint:   "org-b-fingerprint",
				LastCheckedAt: now,
			})
			Expect(err).ToNot(HaveOccurred())

			gotA, err := storeInst.SyncState().Get(ctx, orgId, "git:shared-name/main")
			Expect(err).ToNot(HaveOccurred())
			Expect(gotA.Fingerprint).To(Equal("org-a-fingerprint"))

			gotB, err := storeInst.SyncState().Get(ctx, otherOrgId, "git:shared-name/main")
			Expect(err).ToNot(HaveOccurred())
			Expect(gotB.Fingerprint).To(Equal("org-b-fingerprint"))

			listA, err := storeInst.SyncState().List(ctx, orgId)
			Expect(err).ToNot(HaveOccurred())
			Expect(listA).To(HaveLen(1))
			Expect(listA[0].Fingerprint).To(Equal("org-a-fingerprint"))

			listB, err := storeInst.SyncState().List(ctx, otherOrgId)
			Expect(err).ToNot(HaveOccurred())
			Expect(listB).To(HaveLen(1))
			Expect(listB[0].Fingerprint).To(Equal("org-b-fingerprint"))
		})
	})
})
