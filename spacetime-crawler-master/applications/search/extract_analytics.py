from collections import Counter
import sys

def writeAnalytics(dict):
    try:
        outfile = open("analytics.txt","w")
        outfile.write("---- Subdomain Analytics ----\n")
        for subdomainTuple in dict.most_common():
            outfile.write(str(subdomainTuple[0]) + " links processed: " + str(subdomainTuple[1]) + "\n")
    except Exception as e:
        print(e)
    finally:
        print("data written to analytics.txt...")
        outfile.close()


def extract_info(filePath):
    try:
        subdomainCounter = Counter()

        print("Opening file at path: " + str(filePath))
        infile = open(filePath,"r")
        fileContent = infile.readlines()

        for line in fileContent:
            splitLine = line.split()
            if "incomplete" in splitLine or "download_complete" in splitLine:
                continue
            subdomainCutOffIndex = line.find(".ics")

            subdomain = line[0:subdomainCutOffIndex]
            if subdomain != "www":
                subdomainCounter[subdomain] += 1

        writeAnalytics(subdomainCounter)

    except Exception as e:
        print(e)
    finally:
        print("Closing frontier summary file...")
        infile.close()



def run(systemArguments):
    if len(systemArguments) != 2:
        raise ValueError("The command line arguments are invalid")

    extract_info(systemArguments[1])

if __name__ == "__main__":
    run(sys.argv)
