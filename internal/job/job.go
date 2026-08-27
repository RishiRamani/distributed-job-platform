package job

type Job struct{
	ID		 string `json:"id"`
	Type	 string `json:"type"`
	Status string `json:"status"`
	Duration int `json:"duration"`
	Retries int `json:"retries"`
}

type CreateJobRequest struct{
	Type string `json:"type"`
	Duration int `json:"duration"`
}