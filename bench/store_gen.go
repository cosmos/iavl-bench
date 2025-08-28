package bench

func BankLikeGenerator(seed int64, versions int64) StoreParams {
	return StoreParams{
		StoreKey:         "bank",
		Seed:             seed,
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

func LockupLikeGenerator(seed int64, versions int64) StoreParams {
	return StoreParams{
		StoreKey:         "lockup",
		Seed:             seed,
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

func StakingLikeGenerator(seed int64, versions int64) StoreParams {
	return StoreParams{
		StoreKey:         "staking",
		Seed:             seed,
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

func OsmoLikeGenerators() []StoreParams {
	initialSize := 20_000_000
	finalSize := int(1.5 * float64(initialSize))
	var seed int64 = 1234
	var versions int64 = 1_000_000
	bankGen := BankLikeGenerator(seed, versions)
	bankGen.InitialSize = initialSize
	bankGen.FinalSize = finalSize
	bankGen2 := BankLikeGenerator(seed+1, versions)
	bankGen2.InitialSize = initialSize
	bankGen2.FinalSize = finalSize

	return []StoreParams{
		bankGen,
		bankGen2,
	}
}

func OsmoLikeIterator() ChangesetIterator {
	itr, err := NewChangesetIterators(OsmoLikeGenerators())
	if err != nil {
		panic(err)
	}
	return itr
}
