/**
 * @file This plugin is to access classification image data from different sources. Make sure that
 * the data is conform to expectation.
 */

import { DataSourceApi, generateId, download, unZipData, ImageDataSourceMeta, DataSourceType, Sample } from "@pipcook/pipcook-core"
import glob from 'glob-promise';
import * as path from 'path';
import * as assert from 'assert';
import * as fs from 'fs-extra';
import { Image as I } from '@pipcook/datacook';
const Image = I.default;
type ImageSample = Sample<I.default>;
/**
 * collect the data either from remote url or local file system. It expects a zip
 * which contains the structure of traditional image classification data folder.
 *
 * The structure should be:
 * - train
 *  - category1-name
 *    - image1.jpg
 *    - image2.jpe
 *    - ...
 *  - category2-name
 *  - ...
 * - test (optional)
 * - validate (optional)
 *
 * @param url path of the data, if it comes from local file, please add file:// as prefix
 */
const imageClassDataCollect = async (options: Record<string, any>): Promise<DataSourceApi<I.default>> => {
  let {
    url = '',
    dataDir
  } = options;

  await fs.ensureDir(dataDir);

  assert.ok(url, 'Please specify the url of your dataset');

  const fileName = url.split(path.sep)[url.split(path.sep).length - 1];
  const extention = fileName.split('.');

  assert.ok(extention[extention.length - 1] === 'zip', 'The dataset provided should be a zip file');

  let isDownload = false;

  let targetPath: string;
  if (/^file:\/\/.*/.test(url)) {
    targetPath = url.substring(7);
  } else {
    console.log('downloading dataset ...');
    targetPath = path.join(dataDir, generateId() + '.zip');
    await download(url, targetPath);
    isDownload = true;
  }

  const imageDir = path.join(dataDir, 'images');
  console.log('unzip and collecting data...');
  await unZipData(targetPath, imageDir);
  await fs.remove(targetPath);
  const imagePaths = await glob(path.join(imageDir, '**', '+(train|validation|test)', '*', '*.+(jpg|jpeg|png)'));

  // TODO utils for making dataset
  const train: any[] = [];
  let trainOffset = 0;
  const test: any[] = [];
  let testOffset = 0;
  const categories: Array<string> = [];

  for (const imagePath of imagePaths) {
    const splitString = imagePath.split(path.sep);
    const trainType = splitString[splitString.length - 3];
    const category = splitString[splitString.length - 2];

    let categoryIndex = categories.findIndex((value) => value === category);
    if (categoryIndex === -1) {
      categoryIndex = categories.length;
      categories.push(category);
    }

    if (trainType == 'train') {
      train.push({ data: imagePath, label: categoryIndex});
    } else if (trainType == 'test') {
      test.push({ data: imagePath, label: categoryIndex});
    }
  }

  const sample = await Image.read(train[0].data);

  const meta: ImageDataSourceMeta = {
    type: DataSourceType.Image,
    size: {
      train: train.length,
      test: train.length
    },
    dimension: {
      x: sample.width,
      y: sample.height,
      // TODO: get via API
      z: 3
    },
    //@ts-ignore
    labelMap: categories
  }

  const nextTest = async (): Promise<ImageSample | null > => {
    const sample = test[testOffset++];
    return sample ? {
      data: await Image.read(sample.data),
      label: sample.label
    } : null;
  }

  const nextTrain = async (): Promise<ImageSample | null > => {
    const sample = train[trainOffset++];
    return sample ? {
      data: await Image.read(sample.data),
      label: sample.label
    } : null;
  }

  const nextBatchTest = async (batchSize: number): Promise<Array<ImageSample> | null > => {
    const buffer = [];
    while (batchSize) {
      buffer.push(nextTest());
      batchSize --;
    }
    return Promise.all(buffer);
  }

  const nextBatchTrain = async (batchSize: number): Promise<Array<ImageSample> | null > => {
    const buffer = [];
    while (batchSize) {
      buffer.push(nextTrain());
      batchSize --;
    }
    return Promise.all(buffer);
  }

  const seekTest = async (pos: number) => {
    testOffset = pos;
  }

  const seekTrain = async (pos: number) => {
    trainOffset = pos;
  }

  return {
    getDataSourceMeta: async () => meta,
    nextTest,
    nextTrain,
    nextBatchTest,
    nextBatchTrain,
    seekTest,
    seekTrain
  }
};

export default imageClassDataCollect;
