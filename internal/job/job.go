package job

type Job struct{
	ID		 string `json:"id"`
	Type	 string `json:"type"`
	Status string `json:"status"`
	Duration int `json:"duration"`
	Retries int `json:"retries"`
}

type JobClaim struct{
	Job Job
	WorkerId string
}