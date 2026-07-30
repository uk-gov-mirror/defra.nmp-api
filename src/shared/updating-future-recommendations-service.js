const { MoreThan } = require("typeorm");
const { CropEntity } = require("../db/entity/crop.entity");
const { InprogressCalculationsEntity } = require("../db/entity/inprogress-calculations-entity");
const RB209ArableService = require("../vendors/rb209/arable/arable.service");
const { GenerateRecommendations } = require("./generate-recomendations-service");
const { AppDataSource } = require("../db/data-source");

class UpdatingFutureRecommendations {
  constructor() {
    this.farmExistRepository = AppDataSource.getRepository(
      InprogressCalculationsEntity,
    );
    this.cropRepository = AppDataSource.getRepository(CropEntity);
    this.rB209ArableService = new RB209ArableService();
    this.generateRecommendations = new GenerateRecommendations();
  }

  async getYearsGreaterThanGivenYear(fieldID, year) {
    const years = await this.cropRepository.find({
      where: { FieldID: fieldID, Year: MoreThan(year) },
      select: ["Year"],
    });

    // Dedup — a field can have multiple crop records in the same year
    // (e.g. main + cover crop), which would otherwise cause that year
    // to be regenerated redundantly multiple times.
    return [...new Set(years.map((record) => record.Year))];
  }

  async updateRecommendationsForField(fieldID, year, ctx, userId) {
    const yearsGreaterThanGivenYear = await this.getYearsGreaterThanGivenYear(fieldID, year);
    const allYearsTogether = [...new Set([year, ...yearsGreaterThanGivenYear])];

    await this.processYearsInBackground(fieldID, allYearsTogether, ctx, userId);
  }

  async processYearsInBackground(fieldID, years, ctx, userId) {
    for (const yearToSave of years) {
      try {
        const existingEntry = await this.farmExistRepository.findOne({
          where: { FieldID: fieldID, Year: yearToSave },
        });

        if (!existingEntry) {
          await this.farmExistRepository.save({ FieldID: fieldID, Year: yearToSave });
          console.log(`Saved entry for FieldID: ${fieldID}, Year: ${yearToSave}`);
        }
      } catch (error) {
        console.error(`Error saving entry for FieldID: ${fieldID}, Year: ${yearToSave}`, error);
      }
    }

    // Every year with a plan gets regenerated — this is the business
    // requirement (current year + all future years with plans), not
    // a bug — do not filter this down.
    for (const yearToUpdate of years) {
      try {
        await this.updateRecommendationAndOrganicManure(fieldID, yearToUpdate, ctx, userId);
        console.log(`Successfully processed year ${yearToUpdate} for FieldID ${fieldID}`);
      } catch (error) {
        console.error(`Error processing year ${yearToUpdate} for FieldID ${fieldID}`, error);
      }
    }
  }

  async updateRecommendationAndOrganicManure(fieldID, year, ctx, userId) {
    return AppDataSource.transaction(async (transactionalManager) => {
      const newOrganicManure = null;
      await this.generateRecommendations.generateRecommendations(
        fieldID,
        year,
        newOrganicManure,
        transactionalManager,
        ctx,
        userId
      );

      await transactionalManager.delete(InprogressCalculationsEntity, {
        FieldID: fieldID,
        Year: year,
      });
      console.log(`Deleted entry for FieldID: ${fieldID}, Year: ${year}`);
    });
  }
}

module.exports = { UpdatingFutureRecommendations };
