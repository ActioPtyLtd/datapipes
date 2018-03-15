package actio.datapipes.dataSources

import oshi.SystemInfo


class OSDataSource {
  def test(): Unit = {
    val si = new SystemInfo()
    val os = si.getOperatingSystem
    //val processor = si.getHardware.getMemory.

    //Console.println(processor.getPhysicalProcessorCount)

  }
}
