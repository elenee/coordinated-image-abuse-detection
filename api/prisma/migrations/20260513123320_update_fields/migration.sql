-- AlterTable
ALTER TABLE "Analysis" ALTER COLUMN "harmScore" DROP NOT NULL,
ALTER COLUMN "verdict" DROP NOT NULL,
ALTER COLUMN "latencyMs" DROP NOT NULL;

-- AlterTable
ALTER TABLE "Job" ADD COLUMN     "status" TEXT NOT NULL DEFAULT 'queued';
