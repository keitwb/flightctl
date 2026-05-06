package store_test

import (
	"context"

	"github.com/flightctl/flightctl/internal/config"
	"github.com/flightctl/flightctl/internal/store"
	"github.com/flightctl/flightctl/internal/store/model"
	flightlog "github.com/flightctl/flightctl/pkg/log"
	testutil "github.com/flightctl/flightctl/test/util"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"
)

func strPtr(s string) *string { return &s }

var _ = Describe("DependencyRefStore", func() {
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

	Context("When upserting and listing dependency refs", func() {
		It("should store and retrieve a git ref by fleet", func() {
			ref := &model.DependencyRef{
				OrgID:          orgId,
				FleetName:      strPtr("fleet-a"),
				DeviceName:     strPtr(""),
				RefType:        "git",
				RepositoryName: strPtr("my-repo"),
				Revision:       strPtr("main"),
				SyncInterval:   strPtr("5m"),
			}
			err := storeInst.DependencyRef().Upsert(ctx, orgId, ref)
			Expect(err).ToNot(HaveOccurred())

			refs, err := storeInst.DependencyRef().ListByFleet(ctx, orgId, "fleet-a")
			Expect(err).ToNot(HaveOccurred())
			Expect(refs).To(HaveLen(1))
			Expect(*refs[0].RepositoryName).To(Equal("my-repo"))
			Expect(*refs[0].Revision).To(Equal("main"))
			Expect(refs[0].RefType).To(Equal("git"))
		})

		It("should upsert an existing ref updating mutable fields", func() {
			ref := &model.DependencyRef{
				OrgID:          orgId,
				FleetName:      strPtr("fleet-a"),
				DeviceName:     strPtr(""),
				RefType:        "git",
				RepositoryName: strPtr("my-repo"),
				Revision:       strPtr("main"),
				SyncInterval:   strPtr("5m"),
			}
			err := storeInst.DependencyRef().Upsert(ctx, orgId, ref)
			Expect(err).ToNot(HaveOccurred())

			ref.Revision = strPtr("develop")
			ref.SyncInterval = strPtr("10m")
			err = storeInst.DependencyRef().Upsert(ctx, orgId, ref)
			Expect(err).ToNot(HaveOccurred())

			refs, err := storeInst.DependencyRef().ListByFleet(ctx, orgId, "fleet-a")
			Expect(err).ToNot(HaveOccurred())
			Expect(refs).To(HaveLen(1))
			Expect(*refs[0].Revision).To(Equal("develop"))
			Expect(*refs[0].SyncInterval).To(Equal("10m"))
		})

		It("should store and retrieve a secret ref by device", func() {
			ref := &model.DependencyRef{
				OrgID:           orgId,
				FleetName:       strPtr(""),
				DeviceName:      strPtr("device-1"),
				RefType:         "secret",
				RepositoryName:  strPtr(""),
				SecretName:      strPtr("my-secret"),
				SecretNamespace: strPtr("default"),
				SyncInterval:    strPtr("1m"),
			}
			err := storeInst.DependencyRef().Upsert(ctx, orgId, ref)
			Expect(err).ToNot(HaveOccurred())

			refs, err := storeInst.DependencyRef().ListByDevice(ctx, orgId, "device-1")
			Expect(err).ToNot(HaveOccurred())
			Expect(refs).To(HaveLen(1))
			Expect(refs[0].RefType).To(Equal("secret"))
			Expect(*refs[0].SecretName).To(Equal("my-secret"))
		})
	})

	Context("When listing by ref type", func() {
		It("should return only refs of the requested type", func() {
			gitRef := &model.DependencyRef{
				OrgID:          orgId,
				FleetName:      strPtr("fleet-a"),
				DeviceName:     strPtr(""),
				RefType:        "git",
				RepositoryName: strPtr("repo-1"),
				SyncInterval:   strPtr("5m"),
			}
			secretRef := &model.DependencyRef{
				OrgID:           orgId,
				FleetName:       strPtr("fleet-a"),
				DeviceName:      strPtr(""),
				RefType:         "secret",
				RepositoryName:  strPtr(""),
				SecretName:      strPtr("sec-1"),
				SecretNamespace: strPtr("ns-1"),
			}
			Expect(storeInst.DependencyRef().Upsert(ctx, orgId, gitRef)).To(Succeed())
			Expect(storeInst.DependencyRef().Upsert(ctx, orgId, secretRef)).To(Succeed())

			refs, err := storeInst.DependencyRef().ListByRefType(ctx, orgId, "git")
			Expect(err).ToNot(HaveOccurred())
			Expect(refs).To(HaveLen(1))
			Expect(refs[0].RefType).To(Equal("git"))
		})
	})

	Context("When listing by resource key for fan-out", func() {
		It("should return all fleets referencing a specific secret", func() {
			for _, fleet := range []string{"fleet-a", "fleet-b"} {
				ref := &model.DependencyRef{
					OrgID:           orgId,
					FleetName:       strPtr(fleet),
					DeviceName:      strPtr(""),
					RefType:         "secret",
					RepositoryName:  strPtr(""),
					SecretName:      strPtr("shared-secret"),
					SecretNamespace: strPtr("default"),
				}
				Expect(storeInst.DependencyRef().Upsert(ctx, orgId, ref)).To(Succeed())
			}

			refs, err := storeInst.DependencyRef().ListByResourceKey(ctx, orgId, "secret", map[string]string{
				"secret_name":      "shared-secret",
				"secret_namespace": "default",
			})
			Expect(err).ToNot(HaveOccurred())
			Expect(refs).To(HaveLen(2))
		})
	})

	Context("When deleting stale refs", func() {
		It("should remove refs not in the keep list", func() {
			gitRef := &model.DependencyRef{
				OrgID:          orgId,
				FleetName:      strPtr("fleet-a"),
				DeviceName:     strPtr(""),
				RefType:        "git",
				RepositoryName: strPtr("repo-1"),
				SyncInterval:   strPtr("5m"),
			}
			httpRef := &model.DependencyRef{
				OrgID:          orgId,
				FleetName:      strPtr("fleet-a"),
				DeviceName:     strPtr(""),
				RefType:        "http",
				RepositoryName: strPtr("http-repo-1"),
				HTTPSuffix:     strPtr("/config.json"),
			}
			Expect(storeInst.DependencyRef().Upsert(ctx, orgId, gitRef)).To(Succeed())
			Expect(storeInst.DependencyRef().Upsert(ctx, orgId, httpRef)).To(Succeed())

			fleetName := "fleet-a"
			err := storeInst.DependencyRef().DeleteStaleRefs(ctx, orgId, &fleetName, nil, []string{"git"})
			Expect(err).ToNot(HaveOccurred())

			refs, err := storeInst.DependencyRef().ListByFleet(ctx, orgId, "fleet-a")
			Expect(err).ToNot(HaveOccurred())
			Expect(refs).To(HaveLen(1))
			Expect(refs[0].RefType).To(Equal("git"))
		})
	})

	Context("When deleting by owner", func() {
		It("should remove all refs for a fleet", func() {
			for _, refType := range []string{"git", "secret"} {
				ref := &model.DependencyRef{
					OrgID:          orgId,
					FleetName:      strPtr("fleet-a"),
					DeviceName:     strPtr(""),
					RefType:        refType,
					RepositoryName: strPtr("repo-" + refType),
				}
				Expect(storeInst.DependencyRef().Upsert(ctx, orgId, ref)).To(Succeed())
			}

			fleetName := "fleet-a"
			err := storeInst.DependencyRef().DeleteByOwner(ctx, orgId, &fleetName, nil)
			Expect(err).ToNot(HaveOccurred())

			refs, err := storeInst.DependencyRef().ListByFleet(ctx, orgId, "fleet-a")
			Expect(err).ToNot(HaveOccurred())
			Expect(refs).To(BeEmpty())
		})
	})

	Context("When enforcing org isolation", func() {
		It("should not return refs from a different org", func() {
			otherOrgId := uuid.New()
			err := testutil.CreateTestOrganization(ctx, storeInst, otherOrgId)
			Expect(err).ToNot(HaveOccurred())

			ref := &model.DependencyRef{
				OrgID:          orgId,
				FleetName:      strPtr("fleet-shared"),
				DeviceName:     strPtr(""),
				RefType:        "git",
				RepositoryName: strPtr("repo-1"),
			}
			Expect(storeInst.DependencyRef().Upsert(ctx, orgId, ref)).To(Succeed())

			refs, err := storeInst.DependencyRef().ListByFleet(ctx, otherOrgId, "fleet-shared")
			Expect(err).ToNot(HaveOccurred())
			Expect(refs).To(BeEmpty())
		})
	})
})
