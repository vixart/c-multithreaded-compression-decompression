using System;
using System.IO;
using System.IO.Compression;
using System.Threading;

namespace MtCompressing
{
    class Compressor
    {
        static int threadsCount = Environment.ProcessorCount;
        static int defaultDataSize = Environment.SystemPageSize / threadsCount;
        byte[][] dataArray, compressedDataArray;

        static void Main(string[] args)
        {
            Compressor compressor = new Compressor();
            if (args.Length != 3)
                return;

            switch(args[0])
            {
                case "compress":
                    compressor.Compress(args[1], args[2]);
                    break;
                case "decompress":
                    compressor.Decompress(args[1], args[2]);
                    break;
                default:
                    Console.WriteLine("ERROR: Неверная команда");
                    return;
            }

            return;
        }

        public void Compress(string inName, string outName)
        {
            try
            {
                if(!File.Exists(inName))
                {
                    Console.WriteLine("File does not exist");
                    return;
                }

                if (File.Exists(outName + ".gz"))
                    File.Delete(outName + ".gz");
                
                FileStream inFile = new FileStream(inName, FileMode.Open);
                FileStream outFile = new FileStream(outName + ".gz", FileMode.Append);
                Thread[] threads;
                
                dataArray = new byte[threadsCount][];
                compressedDataArray = new byte[threadsCount][];
                int currentDataSize;

                Console.WriteLine("Start Comressing");
                while (inFile.Position < inFile.Length)
                {
                    threads = new Thread[threadsCount];

                    for (int cycleCount = 0; (cycleCount < threadsCount) && (inFile.Position < inFile.Length); cycleCount++)
                    {
                        // Определяю размер данных для чтения из потока
                        if (inFile.Length - inFile.Position <= defaultDataSize)
                            currentDataSize = (int)(inFile.Length - inFile.Position);
                        else
                            currentDataSize = defaultDataSize;

                        // Читаю блок данных из потока в массив
                        dataArray[cycleCount] = new byte[currentDataSize];
                        inFile.Read(dataArray[cycleCount], 0, currentDataSize);

                        // Запускаю поток архивации блока данных
                        threads[cycleCount] = new Thread(CompressBlock);
                        threads[cycleCount].Start(cycleCount);
                    }

                    for (int cycleCount = 0; (cycleCount < threadsCount) && (threads[cycleCount] != null);)
                    {
                        if (threads[cycleCount].ThreadState == ThreadState.Stopped)
                        {
                            BitConverter.GetBytes(compressedDataArray[cycleCount].Length)
                                           .CopyTo(compressedDataArray[cycleCount], 4);
                            outFile.Write(compressedDataArray[cycleCount], 0, compressedDataArray[cycleCount].Length);
                            cycleCount++;
                        }
                    }
                }

                outFile.Close();
                inFile.Close();
                Console.WriteLine("End Compressing");
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR:" + ex.Message);
            }
        }

        private void CompressBlock(Object cycleNumber)
        {
            using (MemoryStream output = new MemoryStream(dataArray[(int)cycleNumber].Length))
            {
                using (GZipStream cs = new GZipStream(output, CompressionMode.Compress))
                {
                    cs.Write(dataArray[(int)cycleNumber], 0, dataArray[(int)cycleNumber].Length);
                }

                compressedDataArray[(int)cycleNumber] = output.ToArray();
            }
        }

        public void Decompress(string inName, string outName)
        {
            if (!File.Exists(inName))
            {
                Console.WriteLine("File does not exist");
                return;
            }
                
            if (File.Exists(outName))
                File.Delete(outName);

            FileStream inFile = new FileStream(inName, FileMode.Open);
            FileStream outFile = new FileStream(outName, FileMode.Append);
            Thread[] threads;

            dataArray = new byte[threadsCount][];
            compressedDataArray = new byte[threadsCount][];
            byte[] buffer = new byte[8];
            int compressedBlockLength = 0;
            uint currentDataSize;

            Console.WriteLine("Start Decomressing");
            try
            {
                while (inFile.Position < inFile.Length)
                {
                    threads = new Thread[threadsCount];
                    for (int cycleCount = 0; (cycleCount < threadsCount) && (inFile.Position < inFile.Length);
                            cycleCount++)
                    {
                        inFile.Read(buffer, 0, 8);
                        compressedBlockLength = BitConverter.ToInt32(buffer, 4);
                        compressedDataArray[cycleCount] = new byte[compressedBlockLength];
                        buffer.CopyTo(compressedDataArray[cycleCount], 0);

                        inFile.Read(compressedDataArray[cycleCount], 8, compressedBlockLength - 8);
                        currentDataSize = BitConverter.ToUInt32(compressedDataArray[cycleCount], compressedBlockLength - 4);
                        dataArray[cycleCount] = new byte[currentDataSize];
                        
                        threads[cycleCount] = new Thread(DecompressBlock);
                        threads[cycleCount].Start(cycleCount);
                    }

                    for (int cycleCount = 0; (cycleCount < threadsCount) && (threads[cycleCount] != null);)
                    {
                        if (threads[cycleCount].ThreadState == ThreadState.Stopped)
                        {
                            outFile.Write(dataArray[cycleCount], 0, dataArray[cycleCount].Length);
                            cycleCount++;
                        }
                    }
                }
            }
            catch(Exception ex)
            {
                Console.WriteLine("ERROR:" + ex.Message);
            }

            outFile.Close();
            inFile.Close();
            Console.WriteLine("End Decompressing");
        }

        private void DecompressBlock(object cycleCount)
        {
            using (MemoryStream input = new MemoryStream(compressedDataArray[(int)cycleCount]))
            {
                using (GZipStream ds = new GZipStream(input, CompressionMode.Decompress))
                {
                    ds.Read(dataArray[(int)cycleCount], 0, dataArray[(int)cycleCount].Length);
                }
            }
        }
    }
}