package core

func BankLikeGenerator(seed int64, versions int64) ChangesetGenerator {
	return ChangesetGenerator{
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

func LockupLikeGenerator(seed int64, versions int64) ChangesetGenerator {
	return ChangesetGenerator{
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
