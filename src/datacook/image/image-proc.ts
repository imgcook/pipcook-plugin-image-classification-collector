import Jimp from 'jimp';

/**
 * Image class contains utility to handle image manipulation
 * @contructor (data: Jimp)
 */
export class Image {
  private img: Jimp; //img accessible to all methods in the class
  private tf: any;

  constructor (data: Jimp, tf?: any) {
    this.img = data;
    this.tf = tf
  }

  /**
   * Read image from files and create an Image object for processing
   * @param name image file name
   */
  static async read(name: string, tf: any): Promise<Image> {

    const jimpObj = await Jimp.read(name);

    return new Image(jimpObj, tf);
  }

  /**
   * Convert image Buffer to Tensor
   * @param depth -> number of channel to obtain from image
   * @return tf.Tensor3D
   */
  // @ts-ignore
  public toTensor(depth = 3): any {
    // @ts-ignore
    return this.tf.browser.fromPixels({
      data: this.data,
      height: this.height,
      width: this.width
    }, depth);

  }

  /**
   * Convert Image Tensor to one dimensional Array to be converted to Buffer.
   * @param tensor Tensor3D of image
   * @returns Promise<Image>
   */
  // @ts-ignore
  static async fromTensor(tensor: any): Promise<Image> {
    // @ts-ignore
    const imgArray = await this.tf.browser.toPixels(tensor);
    const data: Buffer = Buffer.from(imgArray);
    const [ width, height ] = tensor.shape.slice(0, 2);
    const img: Jimp = await new Jimp({ data:data, width:width, height:height });
    return new Image(img);
  }

  /**
   * Resize image
   * @param width number
   * @param height number
   * @return Image
   */
  public resize(width:number, height:number): Image {
    this.img = this.img.resize(width, height);

    return this;
  }

  get width(): number {
    return this.img.bitmap.width;
  }

  get height(): number {
    return this.img.bitmap.height;
  }

  get data(): Buffer {
    return this.img.bitmap.data;
  }

  /**
   * save the image into a file
   * @param name the image file name
   * @return Boolean
   */
  public save(name:string): boolean {

    const isSave = this.img.write(name);
    if (isSave){
      return true;
    }
    return false;
  }

  /**
   * Rotate image
   * @param deg rotation degree
   * @return Image
   */
  public rotate(deg:number): Image {
    this.img = this.img.rotate(deg);
    return this;
  }

  /**
   * flip the image to horizontal or vertical
   * @param horz
   * @param vert
   * @return Image
   */
  public flip(horz: boolean, vert: boolean): Image {
    this.img = this.img.flip(horz, vert);
    return this;
  }

  /**
   * Crop the desired side of an image
   * @param x
   * @param y
   * @param width
   * @param height
   * @return Image
   */
  public crop(x:number, y:number, width:number, height:number): Image {
    this.img = this.img.crop(x, y, width, height);
    return this;
  }

  /**
   * Un-normalize a normalize image tensor
   * @param data
   * @param mean
   * @param std
   * @returns Tensor
   */
  // @ts-ignore
  static unnormalize(data: any, mean: number, std:number): any {
    // @ts-ignore
    const unnorm = this.tf.cast(data.mul(std).add(mean), "int32");
    // @ts-ignore
    return unnorm as tf.Tensor3D;
  }

}
