package cli

import (
	"context"
	"errors"
	"fmt"
	"strings"

	apiclient "github.com/flightctl/flightctl/internal/api/client"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

type APIOptions struct {
	GlobalOptions
	Method  string
	Headers [][2]string
	Verbose bool
}

func DefaultAPIOptions() *APIOptions {
	return &APIOptions{
		GlobalOptions: DefaultGlobalOptions(),
	}
}

func NewCmdAPI() *cobra.Command {
	o := DefaultAPIOptions()
	cmd := &cobra.Command{
		Use:   "api /path/to/call",
		Short: "Call an API endpoint with optional input data",
		Long: `Call arbitrary API endpoints on the main API server:

Examples:
`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.Complete(cmd, args); err != nil {
				return err
			}
			if err := o.Validate(args); err != nil {
				return err
			}
			ctx, cancel := o.WithTimeout(cmd.Context())
			defer cancel()
			return o.Run(ctx, args)
		},
		SilenceUsage: true,
	}
	o.Bind(cmd.Flags())
	return cmd
}

func (o *APIOptions) Bind(fs *pflag.FlagSet) {
	o.GlobalOptions.Bind(fs)
	fs.FuncP("header", "H", "Header key:value to add to the request", func(s string) error {
		key, value, ok := strings.Cut(s, ":", 1)
		if !ok {
			return errors.New("header must be in key:value format")
		}
		o.Headers = append(o.Headers, [2]string{key, value})
		return nil
	})
	fs.StringVarP(&o.Method, "method", "X", "GET", "HTTP Method to use (e.g. GET, POST)")
	fs.BoolVarP(&o.Verbose, "verbose", "v", false, "Dump raw HTTP request and response with output")
}

func (o *APIOptions) Complete(cmd *cobra.Command, args []string) error {
	if err := o.GlobalOptions.Complete(cmd, args); err != nil {
		return err
	}
	return nil
}

func (o *APIOptions) Validate(args []string) error {
	if err := o.GlobalOptions.Validate(args); err != nil {
		return err
	}

	return nil
}

func (o *APIOptions) Run(ctx context.Context, args []string) error {
	c, err := o.BuildClient()
	organization := o.GetEffectiveOrganization()
	defaultOpts := []apiclient.ClientOption{apiclient.WithHTTPClient(httpClient), ref, client.WithOrganization(organization)}
	defaultOpts = append(defaultOpts, opts...)
	apiClient, err := client.NewClientWithResponses(JoinServerURL(config.Service.Server, client.ServerUrlApiv1), defaultOpts...)
	if err != nil {
		return nil, err
	}
	return apiclient.NewClient(o.ConfigFilePath, client.WithOrganization(organization), client.WithUserAgentHeader("flightctl-cli"))
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}
	c.Start(ctx)
	defer c.Stop()

	c
	return o.runSingleAPI(ctx, c.ClientWithResponses, name)
}
