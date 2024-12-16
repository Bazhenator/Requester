package dto

type RequestStat struct {
	Id            uint64
	GeneratorId   uint64
	TeamId        uint64
	Priority      uint32
	TimeInCleaner float64
	TimeInBuffer  float64
}

type TeamStat struct {
	Id                 uint64
	Speed              uint32
	ProccessedRequests uint64
	TotalBusyTime      float64
}