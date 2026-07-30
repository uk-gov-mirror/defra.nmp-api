const { BaseService } = require("../base/base.service");
const { AppDataSource } = require("../db/data-source");
const { ExcessRainfallsEntity } = require("../db/entity/excess-rainfalls.entity");
const boom = require("@hapi/boom");
const { FieldEntity } = require("../db/entity/field.entity");
const { UpdatingFutureRecommendations } = require("../shared/updating-future-recommendations-service");

class ExcessRainfallService extends BaseService {
  constructor() {
    super(ExcessRainfallsEntity);
    this.repository = AppDataSource.getRepository(ExcessRainfallsEntity);
    this.fieldRepository = AppDataSource.getRepository(FieldEntity);
    this.updatingFutureRecommendations = new UpdatingFutureRecommendations();
  }
  async getExcessRainfallByFarmIdAndYear(fieldId, year) {
    const excessRainfall = await this.repository.findOneBy({
      FarmID: fieldId,
      Year: year,
    });
    return { 
      ExcessRainfall: excessRainfall 
    };
  }
  async checkExcessRainfalExists(farmId, year) {
    return this.recordExists({ FarmID: farmId, Year: year });
  }
  async createExcessRainfall(farmId, year, body, userId, request) {
    const exists = await this.checkExcessRainfalExists(farmId, year);
    if (exists) {
      throw boom.conflict(
        "ExcessRainfall already exists with this FarmID and Year"
      );
    }

    return AppDataSource.transaction(async (transactionalManager) => {
      const excessRainfall = this.repository.create({
        ...body,
        FarmID: farmId,
        Year: year,
        CreatedByID: userId,
        CreatedOn: new Date(),
      });

      const ExcessRainfall = await transactionalManager.save(
        ExcessRainfallsEntity,
        excessRainfall
      );

      const fields = await this.fieldRepository.find({ where: { FarmID: farmId } });
      const fieldIds = fields.map((field) => field.ID);
      const ctx = this.extractContext(request);

      // Detach — do not block the transaction/response on this
      setImmediate(() => {
        this.updateRecommendationsForFields(fieldIds, year, ctx, userId);
      });

      return { ExcessRainfall };
    });
  }

  extractContext(request) {
  return {
    correlationId: request?.headers?.["x-correlation-id"],
    authToken: request?.headers?.authorization,
    // Backward-compat: something downstream in the recommendation chain
    // still reads request.headers.* directly. Keeping this shape avoids
    // breaking that until those call sites are updated to use ctx.authToken.
    headers: {
      authorization: request?.headers?.authorization,
      "x-correlation-id": request?.headers?.["x-correlation-id"],
    },
  };
}

  // Bounded-concurrency dispatch across fields — this is what actually
  // stops the DB pool from being saturated by 20+ simultaneous field jobs.
  async updateRecommendationsForFields(fieldIds, year, ctx, userId) {
    const pLimit = require("p-limit");
    const limit = pLimit(3); // tune against your DB pool size (see note below)

    const jobs = fieldIds.map((fieldId) =>
      limit(() =>
        this.updatingFutureRecommendations
          .updateRecommendationsForField(fieldId, year, ctx, userId)
          .catch((err) => {
            console.error(
              `[updateRecommendationsForFields] Failed for FieldID ${fieldId}, Year ${year}`,
              err
            );
          })
      )
    );

    const results = await Promise.allSettled(jobs);
    const failed = results.filter((r) => r.status === "rejected").length;
    console.log(
      `[updateRecommendationsForFields] Done. ${fieldIds.length - failed}/${fieldIds.length} fields succeeded.`
    );
  }

  async updateExcessRainfall(updatedExcessRainfallData, userId, farmId, year, request) {
    const { ID, CreatedByID, CreatedOn, ...dataToUpdate } = updatedExcessRainfallData;

    const result = await this.repository.update(
      { FarmID: farmId, Year: year },
      { ...dataToUpdate, ModifiedByID: userId, ModifiedOn: new Date() }
    );

    if (result.affected === 0) {
      throw boom.notFound(`ExcessRainfall with FarmID ${farmId} and Year ${year} not found`);
    }

    const updatedExcessRainfall = await this.repository.findOne({
      where: { FarmID: farmId, Year: year },
    });

    const fields = await this.fieldRepository.find({ where: { FarmID: farmId } });
    const fieldIds = fields.map((field) => field.ID);
    const ctx = this.extractContext(request);

    // Guarantee this runs AFTER the response is sent, and is bounded/error-safe
    setImmediate(() => {
      this.updateRecommendationsForFields(fieldIds, year, ctx, userId);
    });

    return updatedExcessRainfall;
  }
}

module.exports = { ExcessRainfallService };
