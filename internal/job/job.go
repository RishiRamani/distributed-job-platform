package job

type Job struct{
	ID		 string `json:"id"`
	Type	 string `json:"type"`
	Status string `json:"status"`
	Duration int `json:"duration"`
}

type JobClaim struct{
	Job Job
	WorkerId string
}