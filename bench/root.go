package bench

//func RootCommand(cmds ...*cobra.Command) (*cobra.Command, error) {
//	var (
//		exposeMetrics bool
//		metricsport   int
//	)
//
//	cmd := &cobra.Command{
//		Use:   "iavl-bench",
//		Short: "benchmark cosmos/iavl",
//		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
//			if exposeMetrics {
//				go func() {
//					http.Handle("/metrics", promhttp.Handler())
//					err := http.ListenAndServe(fmt.Sprintf(":%d", metricsport), nil)
//					if err != nil {
//						panic(err)
//					}
//				}()
//			}
//			return nil
//		},
//	}
//
//	cmd.PersistentFlags().String("index-dir", fmt.Sprintf("%s/.costor", os.Getenv("HOME")), "costor home directory")
//	cmd.PersistentFlags().BoolVar(&exposeMetrics, "metrics", false, "enable prometheus metrics")
//	cmd.PersistentFlags().IntVar(&metricsport, "metrics-port", 2112, "prometheus metrics port")
//
//	return cmd, nil
//}
