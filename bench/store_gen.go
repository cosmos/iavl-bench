package bench

func BankLikeGenerator(versions int64) StoreParams {
	return StoreParams{
		StoreKey:         "bank",
		KeyMean:          56,
		KeyStdDev:        3,
		ValueMean:        100,
		ValueStdDev:      1200,
		InitialSize:      35_000,
		FinalSize:        2_200_200,
		Versions:         versions,
		ChangePerVersion: int(int64(368_000_000) / versions),
		DeleteFraction:   0.25,
	}
}

func LockupLikeGenerator(versions int64) StoreParams {
	return StoreParams{
		StoreKey:         "lockup",
		KeyMean:          56,
		KeyStdDev:        3,
		ValueMean:        1936,
		ValueStdDev:      29261,
		InitialSize:      35_000,
		FinalSize:        2_600_200,
		Versions:         versions,
		ChangePerVersion: int(int64(72_560_000) / versions),
		DeleteFraction:   0.29,
	}
}

func StakingLikeGenerator(versions int64) StoreParams {
	return StoreParams{
		StoreKey:         "staking",
		KeyMean:          24,
		KeyStdDev:        2,
		ValueMean:        12263,
		ValueStdDev:      22967,
		InitialSize:      35_000,
		FinalSize:        1_600_696,
		Versions:         versions,
		ChangePerVersion: int(int64(60_975_465) / versions),
		DeleteFraction:   0.25,
	}
}

func OsmoLikeGenerators(scale float64) []StoreParams {
	initialSize := int(20_000_000 * scale)
	finalSize := int(1.5 * float64(initialSize))
	var versions int64 = 1_000_000
	bankGen := BankLikeGenerator(versions)
	bankGen.InitialSize = initialSize
	bankGen.FinalSize = finalSize
	bankGen2 := BankLikeGenerator(versions)
	bankGen2.StoreKey = "bank2"
	bankGen2.InitialSize = initialSize
	bankGen2.FinalSize = finalSize

	return []StoreParams{
		bankGen,
		bankGen2,
	}
}
