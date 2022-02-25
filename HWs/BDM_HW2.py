from mrjob.job import MRJob
from mrjob.step import MRStep


class MRFindReciprocal(MRJob):
  '''
  PLEASE COMPLETE THIS CLASS. THIS SHOULD BE THE ONLY PLACE THAT YOU CAN EDIT.
  THE INPUT OF YOUR MAPREDUCE JOB WOULD BE LINE OF TEXT WITHOUT '\n'.
  '''
  def mapper1(self, _, line):
    row = line.split(',')
    if '@enron.com' in row[1]:
      mailer = row[1].split('@')[0].replace('.', ' ').lower()
      receiver_list = filter(lambda x: '@enron.com' in x, row[2].split(';')) # split the receiver's email
      receiver_list = list(map(lambda x: x.split('@')[0].replace('.', ' ').lower(), receiver_list))
      yield (mailer, receiver_list)

  def reducer1(self, mailer, receiver_list):
    yield (mailer, [item for sublist in receiver_list for item in sublist])

  
  def mapper2(self, mailer, receiver_list):
    receiver_list = set(receiver_list)
    mailer = mailer.title()
    for i in receiver_list:
      i = i.title()
      if mailer < i:
        from_to = (mailer, i)
      else:
        from_to = (i, mailer)
      
      yield (from_to, 1)

  def reducer2(self, from_to, count):
      yield (from_to, sum(count))

  def mapper3(self, from_to, count):
    if count > 1:
      yield ('reciprocal', from_to[0]+' : '+from_to[1])


  def steps(self):
    return [
      MRStep(mapper=self.mapper1, reducer=self.reducer1),
      MRStep(mapper=self.mapper2, reducer=self.reducer2),
      MRStep(mapper=self.mapper3), 
    ]


if __name__ == '__main__':
    MRFindReciprocal.run()