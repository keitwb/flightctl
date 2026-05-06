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

	Context("When upserting dependency refs", func() {
		It("should store and retrieve a ref via upsert", func() {
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
		})

		It("should reject nil ref", func() {
			err := storeInst.DependencyRef().Upsert(ctx, orgId, nil)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("nil ref"))
		})
	})

	Context("When deleting dependency refs", func() {
		It("should delete a ref by composite key", func() {
			ref := &model.DependencyRef{
				OrgID:           orgId,
				FleetName:       strPtr("fleet-a"),
				DeviceName:      strPtr(""),
				RefType:         "git",
				RepositoryName:  strPtr("my-repo"),
				SecretName:      strPtr(""),
				SecretNamespace: strPtr(""),
			}
			err := storeInst.DependencyRef().Upsert(ctx, orgId, ref)
			Expect(err).ToNot(HaveOccurred())

			err = storeInst.DependencyRef().Delete(ctx, orgId, ref)
			Expect(err).ToNot(HaveOccurred())
		})

		It("should reject nil ref", func() {
			err := storeInst.DependencyRef().Delete(ctx, orgId, nil)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("nil ref"))
		})
	})
})
