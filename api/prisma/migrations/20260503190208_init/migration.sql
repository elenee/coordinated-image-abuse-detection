-- CreateTable
CREATE TABLE "Job" (
    "id" TEXT NOT NULL,
    "userId" TEXT NOT NULL,
    "mimeType" TEXT NOT NULL,
    "sizeBytes" INTEGER NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "Job_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "Analysis" (
    "id" TEXT NOT NULL,
    "jobId" TEXT NOT NULL,
    "pHash" TEXT NOT NULL,
    "harmScore" DOUBLE PRECISION NOT NULL,
    "clipSimilarity" DOUBLE PRECISION,
    "similarUsers" TEXT[],
    "burstDetected" BOOLEAN NOT NULL,
    "verdict" TEXT NOT NULL,
    "reasons" TEXT[],
    "latencyMs" INTEGER NOT NULL,
    "report" TEXT,
    "analyzedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "Analysis_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "Analysis_jobId_key" ON "Analysis"("jobId");

-- AddForeignKey
ALTER TABLE "Analysis" ADD CONSTRAINT "Analysis_jobId_fkey" FOREIGN KEY ("jobId") REFERENCES "Job"("id") ON DELETE RESTRICT ON UPDATE CASCADE;
